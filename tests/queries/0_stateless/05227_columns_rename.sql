DROP TABLE IF EXISTS t_columns_rename;

CREATE TABLE t_columns_rename
(
    a UInt8,
    b UInt8,
    metric_cpu UInt8,
    metric_mem UInt8
)
ENGINE = Memory;

INSERT INTO t_columns_rename VALUES (1, 2, 3, 4);

-- RENAME must reject an unqualified source name that matches more than one joined column.
SELECT * RENAME id AS renamed_id
FROM (SELECT toUInt8(1) AS id) AS left_table
CROSS JOIN (SELECT toUInt8(2) AS id) AS right_table; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Qualified matchers must keep working through multiple joins.
SELECT l.* RENAME UserID AS user_id
FROM (SELECT number AS UserID FROM numbers(1)) AS l
LEFT JOIN (SELECT number AS id FROM numbers(1)) AS r ON r.id = l.UserID
LEFT JOIN (SELECT number AS id FROM numbers(1)) AS s ON s.id = l.UserID
FORMAT TSVWithNames;

-- The legacy analyzer must expose RENAME aliases to sibling projection items and clauses.
SELECT x + 1 AS y, * RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME a AS x FROM t_columns_rename ORDER BY x FORMAT TSVWithNames;

-- The modern analyzer must produce the same headers and values.
SET enable_analyzer = 1;

SELECT * RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME a AS x FROM t_columns_rename ORDER BY x FORMAT TSVWithNames;
SELECT * RENAME a AS x, count() FROM t_columns_rename GROUP BY a, b, metric_cpu, metric_mem WITH ROLLUP ORDER BY x LIMIT 1 BY x SETTINGS group_by_use_nulls = 1 FORMAT TSVWithNames;

-- Early RENAME alias collection must not freeze a correlated REPLACE subquery before nullable grouping keys are registered.
SELECT DISTINCT toTypeName(x)
FROM
(
    SELECT COLUMNS('^a$') REPLACE((SELECT a) AS a) RENAME a AS x, count()
    FROM t_columns_rename
    GROUP BY t_columns_rename.a WITH ROLLUP
    SETTINGS group_by_use_nulls = 1, allow_experimental_correlated_subqueries = 1
);

-- Clause lookup must re-resolve the same RENAME alias after GROUP BY.
SELECT COLUMNS('^a$') REPLACE(a + 1 AS a) RENAME a AS x, count()
FROM t_columns_rename
GROUP BY t_columns_rename.a WITH ROLLUP
ORDER BY x ASC NULLS FIRST
SETTINGS group_by_use_nulls = 1
FORMAT TSV;
SELECT * RENAME (a AS x, b AS y) FROM t_columns_rename FORMAT TSVWithNames;
SELECT t_columns_rename.* RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT COLUMNS('^metric_') RENAME (metric_cpu AS cpu, metric_mem AS mem) FROM t_columns_rename FORMAT TSVWithNames;
SELECT * APPLY(toString) RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT * REPLACE(a + 1 AS a) RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT * EXCEPT(b) RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME (a AS b, b AS c) FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME (a AS b, b AS c) FROM t_columns_rename ORDER BY b, c FORMAT TSVWithNames;
SELECT * RENAME a AS a FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME a AS x, * FROM t_columns_rename FORMAT TSVWithNames;
SELECT *, * RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME a AS x, *, * RENAME b AS y FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME `a` AS `new name` FROM t_columns_rename FORMAT TSVWithNames;

SELECT * RENAME id AS renamed_id
FROM (SELECT toUInt8(1) AS id) AS left_table
CROSS JOIN (SELECT toUInt8(2) AS id) AS right_table; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT l.* RENAME UserID AS user_id
FROM (SELECT number AS UserID FROM numbers(1)) AS l
LEFT JOIN (SELECT number AS id FROM numbers(1)) AS r ON r.id = l.UserID
LEFT JOIN (SELECT number AS id FROM numbers(1)) AS s ON s.id = l.UserID
FORMAT TSVWithNames;

SELECT x + 1 AS y, * RENAME a AS x FROM t_columns_rename FORMAT TSVWithNames;
SELECT * RENAME a AS x, x + 1 AS y FROM t_columns_rename FORMAT TSVWithNames;

-- Wrapped RENAME matchers must register aliases before sibling projection items and ORDER BY.
SELECT x + 1 AS y, tuple(* RENAME a AS x) = (1, 2, 3, 4) AS ok FROM t_columns_rename ORDER BY x SETTINGS group_by_use_nulls = 1 FORMAT TSV;

-- GROUP BY ALL must restore a projection that was not resolved by early RENAME alias collection.
SELECT DISTINCT toTypeName(x)
FROM
(
    SELECT COLUMNS('^a$') REPLACE((SELECT toUInt8(1)) AS a) RENAME a AS x, count()
    FROM t_columns_rename
    GROUP BY ALL WITH ROLLUP
    SETTINGS group_by_use_nulls = 1
);

SELECT * RENAME missing AS x FROM t_columns_rename; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT * RENAME (a AS x, a AS y) FROM t_columns_rename; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * RENAME (a AS x, b AS x) FROM t_columns_rename; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * EXCEPT(a) RENAME a AS x FROM t_columns_rename; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT * RENAME () FROM t_columns_rename; -- { clientError SYNTAX_ERROR }
SELECT * RENAME a x FROM t_columns_rename; -- { clientError SYNTAX_ERROR }
SELECT * RENAME a AS x APPLY(toString) FROM t_columns_rename; -- { clientError SYNTAX_ERROR }
SELECT * RENAME a AS x RENAME x AS y FROM t_columns_rename; -- { clientError SYNTAX_ERROR }
INSERT INTO t_columns_rename (* RENAME a AS x) SELECT 1, 2, 3, 4; -- { clientError SYNTAX_ERROR }

DROP TABLE t_columns_rename;
