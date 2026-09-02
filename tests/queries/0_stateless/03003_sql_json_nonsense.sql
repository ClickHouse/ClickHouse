SELECT JSON_QUERY('{"x":1}', '$[\'hello\']', materialize(toLowCardinality('x')));
