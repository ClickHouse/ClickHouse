DROP TABLE IF EXISTS t_alias_subcolumns;

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

CREATE TABLE t_alias_subcolumns
(
    id UInt64,
    a Array(UInt64),
    n Nullable(String),
    aa Array(UInt64) ALIAS a,
    ab Array(UInt64) ALIAS arrayFilter(x -> x % 2 = 0, a),
    na Nullable(String) ALIAS n,
    nb Nullable(String) ALIAS substring(n, 1, 3)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_subcolumns VALUES (0, [1, 2], 'ffffffff') (1, [3], NULL);

SELECT aa.size0 FROM t_alias_subcolumns ORDER BY id;
SELECT ab.size0 FROM t_alias_subcolumns ORDER BY id;

EXPLAIN QUERY TREE SELECT aa.size0 FROM t_alias_subcolumns ORDER BY id;

SELECT na.null FROM t_alias_subcolumns ORDER BY id;
SELECT nb.null FROM t_alias_subcolumns ORDER BY id;

EXPLAIN QUERY TREE SELECT na.null FROM t_alias_subcolumns ORDER BY id;

SELECT count() FROM t_alias_subcolumns WHERE NOT empty(aa);
SELECT count() FROM t_alias_subcolumns WHERE NOT empty(ab);

EXPLAIN QUERY TREE SELECT count() FROM t_alias_subcolumns WHERE NOT empty(aa);

SELECT count(na) FROM t_alias_subcolumns;
SELECT count(nb) FROM t_alias_subcolumns;

EXPLAIN QUERY TREE SELECT count(na) FROM t_alias_subcolumns;

DROP TABLE t_alias_subcolumns;
