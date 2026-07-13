-- Tags: stateful
SELECT sum(DISTINCT sipHash64(decodeXMLComponent(Title) AS decoded)) FROM test.hits WHERE Title != decoded;
