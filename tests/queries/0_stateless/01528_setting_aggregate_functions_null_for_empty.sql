DROP TABLE IF EXISTS defaults;

CREATE TABLE defaults
(
	n Int8
)ENGINE = Memory();

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;

SET aggregate_functions_null_for_empty=1;

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;

INSERT INTO defaults SELECT * FROM numbers(10);

SET aggregate_functions_null_for_empty=0;

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;

SET aggregate_functions_null_for_empty=1;

SELECT sum(n) FROM defaults;
SELECT sumOrNull(n) FROM defaults;
SELECT count(n) FROM defaults;
SELECT countOrNull(n) FROM defaults;


EXPLAIN SYNTAX SELECT sumIf(1, number > 0) FROM numbers(10) WHERE 0;

DROP TABLE defaults;
