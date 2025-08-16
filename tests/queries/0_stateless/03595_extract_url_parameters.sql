-- Tags: no-fasttest
SELECT sum(cityHash64(extractURLParameters(URL))) FROM test.hits;
SELECT sum(cityHash64(extractURLParameterNames(URL))) FROM test.hits;
