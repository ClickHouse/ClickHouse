DROP TABLE IF EXISTS sorted;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE sorted (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 8192);

INSERT INTO sorted (x) SELECT intDiv(number, 100000) AS x FROM system.numbers LIMIT 1000000;

SET max_threads = 1;

SELECT count() FROM sorted;
SELECT DISTINCT x FROM sorted;

INSERT INTO sorted (x) SELECT (intHash64(number) % 1000 = 0 ? 999 : intDiv(number, 100000)) AS x FROM system.numbers LIMIT 1000000;

SELECT count() FROM sorted;
SELECT DISTINCT x FROM sorted;

DROP TABLE sorted;
