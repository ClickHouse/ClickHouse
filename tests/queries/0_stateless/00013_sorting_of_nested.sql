-- Tags: stateful
SELECT ParsedParams.Key1 FROM test.visits FINAL WHERE VisitID != 0 AND notEmpty(ParsedParams.Key1) ORDER BY VisitID LIMIT 10
