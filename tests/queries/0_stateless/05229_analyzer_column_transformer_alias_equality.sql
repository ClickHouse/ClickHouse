-- Two aliases holding a `*` matcher are the same alias only when their column transformers agree in contents.

DROP TABLE IF EXISTS test_transformer_alias;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_transformer_alias;
CREATE TABLE test_transformer_alias (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_transformer_alias VALUES (0, 'Value');

WITH x -> * EXCEPT STRICT (id) AS lambda, x -> * EXCEPT STRICT (id) AS lambda SELECT lambda(1) FROM test_transformer_alias ORDER BY 1;
WITH x -> * EXCEPT ('i.*') AS lambda, x -> * EXCEPT ('i.*') AS lambda SELECT lambda(1) FROM test_transformer_alias ORDER BY 1;

WITH x -> * EXCEPT STRICT (id) AS lambda, x -> * EXCEPT (id) AS lambda SELECT lambda(1) FROM test_transformer_alias; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * REPLACE STRICT (1 AS id) AS lambda, x -> * REPLACE (1 AS id) AS lambda SELECT lambda(1) FROM test_transformer_alias; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * REPLACE (1 AS id) AS lambda, x -> * REPLACE (1 AS value) AS lambda SELECT lambda(1) FROM test_transformer_alias; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * EXCEPT (id) AS lambda, x -> * EXCEPT ('id') AS lambda SELECT lambda(1) FROM test_transformer_alias; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * EXCEPT ('i.*') AS lambda, x -> * EXCEPT ('v.*') AS lambda SELECT lambda(1) FROM test_transformer_alias; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

DROP TABLE test_transformer_alias;
