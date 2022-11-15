-- Tags: no-fasttest

SELECT h3GetDestinationIndexFromUnidirectionalEdge(1248204388774707197);
SELECT h3GetDestinationIndexFromUnidirectionalEdge(599686042433355773);
SELECT h3GetDestinationIndexFromUnidirectionalEdge(stringToH3('85283473ffffff'));

SELECT h3GetIndexesFromUnidirectionalEdge(1248204388774707199);
SELECT h3GetIndexesFromUnidirectionalEdge(599686042433355775);
SELECT h3GetIndexesFromUnidirectionalEdge(stringToH3('85283473ffffff'));

SELECT h3GetOriginIndexFromUnidirectionalEdge(1248204388774707199);
SELECT h3GetOriginIndexFromUnidirectionalEdge(1248204388774707197);
SELECT h3GetOriginIndexFromUnidirectionalEdge(599686042433355775);
SELECT h3GetOriginIndexFromUnidirectionalEdge(stringToH3('85283473ffffff'));

SELECT h3GetUnidirectionalEdgeBoundary(1248204388774707199);
SELECT h3GetUnidirectionalEdgeBoundary(599686042433355773);
SELECT h3GetUnidirectionalEdgeBoundary(stringToH3('85283473ffffff'));

SELECT h3GetUnidirectionalEdgesFromHexagon(1248204388774707199);
SELECT h3GetUnidirectionalEdgesFromHexagon(599686042433355773);
SELECT h3GetUnidirectionalEdgesFromHexagon(stringToH3('85283473ffffff'));

select h3GetUnidirectionalEdge(stringToH3('85283473fffffff'), stringToH3('85283477fffffff'));
select h3GetUnidirectionalEdge(stringToH3('85283473fffffff'), stringToH3('85283473fffffff'));
SELECT h3GetUnidirectionalEdge(stringToH3('85283473ffffff'), stringToH3('852\03477fffffff')); -- { serverError 43 }

SELECT h3UnidirectionalEdgeIsValid(1248204388774707199) as edge;
SELECT h3UnidirectionalEdgeIsValid(1248204388774707197) as edge;
SELECT h3UnidirectionalEdgeIsValid(stringToH3('85283473ffffff')) as edge;
