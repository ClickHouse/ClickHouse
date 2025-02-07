-- Tags: stateful
SELECT sum(length(ParsedParams.Key1)) FROM test.hits WHERE notEmpty(ParsedParams.Key1);
SELECT sum(length(ParsedParams.ValueDouble)) FROM test.hits WHERE notEmpty(ParsedParams.ValueDouble);
