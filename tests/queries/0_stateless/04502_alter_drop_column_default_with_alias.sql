-- Tests that ALTER TABLE ... DROP COLUMN works when another column has a DEFAULT/MATERIALIZED
-- expression that defines and later references an inline alias (`expr AS a ... a`).
-- Previously the dependency check for the dropped column resolved each remaining default expression
-- as a standalone node, which did not collect its internal aliases, so it failed with UNKNOWN_IDENTIFIER.

DROP TABLE IF EXISTS t_04502;

CREATE TABLE t_04502
(
    id UInt64,
    src String,
    to_drop String DEFAULT '',
    aliased String DEFAULT concat(substring(upper(src) AS u, 1, 3), '-', u)
)
ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_04502 DROP COLUMN to_drop;

INSERT INTO t_04502 (id, src) VALUES (1, 'hello');
SELECT id, src, aliased FROM t_04502 ORDER BY id;

DROP TABLE t_04502;

-- A more elaborate case matching the original report: the alias is defined outside a lambda
-- while another alias lives inside the lambda body.

DROP TABLE IF EXISTS t_04502_nested;

CREATE TABLE t_04502_nested
(
    id UInt64,
    url String,
    md5 String MATERIALIZED lower(hex(MD5(url))),
    s3_url String DEFAULT concat('prefix/', substring(arrayStringConcat(arrayMap(i -> substring(lower(hex(MD5(url))) AS hx, i, 1), range(1, 4))) AS h, 1, 2), '/', h)
)
ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_04502_nested DROP COLUMN md5;

INSERT INTO t_04502_nested (id, url) VALUES (1, 'example');
SELECT id, s3_url FROM t_04502_nested ORDER BY id;

DROP TABLE t_04502_nested;
