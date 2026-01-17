-- Tags: no-fasttest, stateful
SELECT sum(cityHash64(extractURLParameters(URL))) FROM test.hits;
SELECT sum(cityHash64(extractURLParameterNames(URL))) FROM test.hits;
