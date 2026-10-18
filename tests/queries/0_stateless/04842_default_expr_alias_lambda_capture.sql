-- Expanding an ALIAS column inside a lambda must not let the lambda parameter capture
-- identifiers from the alias body: they were written at table scope and refer to table columns.

DROP TABLE IF EXISTS t_alias_lambda_capture;

-- `y ALIAS x + 1` references the table column `x`; substituting it into `arrayMap(x -> y, arr)`
-- would rebind it to the lambda parameter `x`. Rejected at CREATE time.
CREATE TABLE t_alias_lambda_capture
(
    x UInt8,
    arr Array(UInt8),
    y UInt8 ALIAS x + 1,
    m Array(UInt8) MATERIALIZED arrayMap(x -> y, arr)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- A transitive chain ending in a captured identifier is rejected too.
CREATE TABLE t_alias_lambda_capture
(
    x UInt8,
    arr Array(UInt8),
    y UInt8 ALIAS x + 1,
    y2 UInt8 ALIAS y * 2,
    m Array(UInt8) MATERIALIZED arrayMap(x -> y2, arr)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- A lambda parameter that does not collide with the alias body is fine:
-- `y` expands to `x + 1` over the table column `x`.
CREATE TABLE t_alias_lambda_capture
(
    x UInt8,
    arr Array(UInt8),
    y UInt8 ALIAS x + 1,
    m Array(UInt8) MATERIALIZED arrayMap(elem -> y + elem, arr)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alias_lambda_capture (x, arr) VALUES (10, [1, 2]);
SELECT m FROM t_alias_lambda_capture;

-- ALTER ADD COLUMN takes the same validation.
ALTER TABLE t_alias_lambda_capture ADD COLUMN m2 Array(UInt8) MATERIALIZED arrayMap(x -> y, arr); -- { serverError BAD_ARGUMENTS }

-- A lambda inside the alias body shadows its own parameters and is not a capture.
ALTER TABLE t_alias_lambda_capture ADD COLUMN y3 UInt8 ALIAS arrayMax(x -> x, arr);
ALTER TABLE t_alias_lambda_capture ADD COLUMN m3 Array(UInt8) MATERIALIZED arrayMap(x -> y3, arr);
INSERT INTO t_alias_lambda_capture (x, arr) VALUES (20, [3, 4]);
SELECT m3 FROM t_alias_lambda_capture ORDER BY x;

DROP TABLE t_alias_lambda_capture;
