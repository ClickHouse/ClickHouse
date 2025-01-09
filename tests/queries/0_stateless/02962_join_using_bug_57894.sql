DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS r;
SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE t (`x` UInt32, `s` LowCardinality(String)) ENGINE = Memory;
INSERT INTO t SELECT number, toString(number) FROM numbers(5);

CREATE TABLE r (`x` LowCardinality(Nullable(UInt32)), `s` Nullable(String)) ENGINE = Memory;
INSERT INTO r SELECT number, toString(number) FROM numbers(2, 8);
INSERT INTO r VALUES (NULL, NULL);

SET allow_experimental_analyzer = 0;

SELECT x FROM t FULL JOIN r USING (x) ORDER BY ALL
;


SELECT x FROM t FULL JOIN r USING (x) ORDER BY ALL
SETTINGS join_algorithm = 'partial_merge';

SELECT x FROM t FULL JOIN r USING (x) ORDER BY ALL
SETTINGS join_algorithm = 'full_sorting_merge';

SET allow_experimental_analyzer = 1;

SELECT x FROM t FULL JOIN r USING (x) ORDER BY ALL
;

SELECT x FROM t FULL JOIN r USING (x) ORDER BY ALL
SETTINGS join_algorithm = 'partial_merge';

SELECT x FROM t FULL JOIN r USING (x) ORDER BY ALL
SETTINGS join_algorithm = 'full_sorting_merge';
