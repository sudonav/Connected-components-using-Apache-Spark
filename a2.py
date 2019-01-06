# Kindly run this file with the arguments for input and output as shown below
# spark-submit a2.pt input output
# input - input file
# output - output folder
from pyspark import SparkConf, SparkContext
import sys

#Emits both the edges (u,v) and (v,u)
def filterAndEmitEdges(edges):
    edgeList = edges.split(" ")
    vertex = [int(edge) for edge in edgeList]
    return [(vertex[0],vertex[1]),(vertex[1],vertex[0])]

#Emits both the edges (u,v) and (v,u)
def emitEdges(edges):
    vertex = [int(edge) for edge in edges]
    return [(vertex[0],vertex[1]),(vertex[1],vertex[0])]

#Emits the minimum amongst the neighbours for every vertex
def emitMinNeighbourOfVertices(inputRDD):
    return inputRDD.reduceByKey(lambda a,b: min(a,b))

#Emits the minimum of the min_neighbour and self for every vertex
def emitMinNeighbourOfVerticesWithSelf(inputRDD):
    return inputRDD.map(lambda x: (x[0], min(x[0],x[1])))

#Emits the large star for the each edge (u,v) in the format [u, [v,m]]
def emitLargeStar(vertex):
        #if(u<v)
    if(vertex[0] < vertex[1][0]):
        #return (v,m)
        return (vertex[1][0],vertex[1][1])

#Emits the Large Star
def computeLargeStar(inputRDD):
    #min(Gamma(u))
    minNeighbourOfVertices = emitMinNeighbourOfVertices(inputRDD)
    #m = min(Gamma+(u))
    m = emitMinNeighbourOfVerticesWithSelf(minNeighbourOfVertices)
    #[u, [v,m]] i.e. inner join with the input based on key(vertex label)
    combinedNeighbourhood = inputRDD.join(m)
    #return largeStar and filtering out the none values
    return combinedNeighbourhood.map(emitLargeStar).filter(lambda a: a is not None)

def emitSmallStarInput(vertex):  
    #if(v<=u)
    if(vertex[1] <= vertex[0]):
        #return (u,v)
        return (vertex[0],vertex[1])
    else:
        #return (v,u)
        return (vertex[1],vertex[0])

#Emits the small star for the each vertex in the format [u, [v,m]]
def emitSmallStar(vertex):
    #if(v==u) there is a self loop, return original edge
    if(vertex[1][0] == vertex[1][1]):
        #return (u,v)
        return (vertex[0], vertex[1][0])
    else:
        #return (v,m), connect vertex to minimum
        return (vertex[1][0], vertex[1][1])

#Emits the Small Star
def computeSmallStar(inputRDD):
    #min(Gamma(u))
    minNeighbourOfVertices = emitMinNeighbourOfVertices(inputRDD)
    #m = min(Gamma+(u))
    m = emitMinNeighbourOfVerticesWithSelf(minNeighbourOfVertices)
    #[u, [v,m]] i.e. inner join with the input based on key(vertex label)
    combinedNeighbourhood = inputRDD.join(m)
    #return small star and filtering out the none values
    return combinedNeighbourhood.map(emitSmallStar).filter(lambda a: a is not None)

#Check for convergence
def checkConverge(V, iterationOutput):
    hasConverged = False
    if(V.subtract(iterationOutput).count() == 0 and iterationOutput.subtract(V).count() == 0):
        hasConverged = True
    return hasConverged

#Eliminate the delimiter ',' and replace with ' ' (space)
def eliminateDelimiter(edges):
    return (str(edges[0])+" "+str(edges[1]))

if __name__ == "__main__":
    conf = SparkConf().setAppName("ConnectedComponents")
    sc = SparkContext(conf = conf)
    hasConverged = False
    lines = sc.textFile(sys.argv[1])
    V = lines.flatMap(filterAndEmitEdges)
    V.persist()

    while(hasConverged == False):
        largeStar = computeLargeStar(V)
        smallStarInput = largeStar.map(emitSmallStarInput)
        smallStar = computeSmallStar(smallStarInput)
        iterationOutput = smallStar.flatMap(emitEdges)
        hasConverged = checkConverge(V,iterationOutput)
        V = iterationOutput
        V.persist()

    #Output is presented in the format 'vertex label'
    output = smallStar.map(eliminateDelimiter).distinct()
    output.saveAsTextFile(sys.argv[2])
    print(sorted(output.collect()))
    sc.stop()
