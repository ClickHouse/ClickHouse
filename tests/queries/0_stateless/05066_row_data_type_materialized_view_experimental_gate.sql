-- A materialized view with an inner table creates that table here, so it must not bypass the Row opt-in.

DROP TABLE IF EXISTS row_mv_gate_src;
DROP TABLE IF EXISTS row_mv_gate_dst;
DROP TABLE IF EXISTS row_mv_gate;

SET allow_experimental_row_type = 1;
CREATE TABLE row_mv_gate_src (a UInt64, r Row(x UInt64, y String)) ENGINE = MergeTree ORDER BY a;

SET allow_experimental_row_type = 0;
CREATE MATERIALIZED VIEW row_mv_gate ENGINE = MergeTree ORDER BY a AS SELECT a, r FROM row_mv_gate_src; -- { serverError ILLEGAL_COLUMN }
ATTACH MATERIALIZED VIEW row_mv_gate UUID '05066000-0000-0000-0000-000000000001' (a UInt64, r Row(x UInt64, y String)) ENGINE = MergeTree ORDER BY a AS SELECT a, r FROM row_mv_gate_src; -- { serverError ILLEGAL_COLUMN }

SET allow_experimental_row_type = 1;
CREATE MATERIALIZED VIEW row_mv_gate ENGINE = MergeTree ORDER BY a AS SELECT a, r FROM row_mv_gate_src;
INSERT INTO row_mv_gate_src VALUES (1, (1, 'x'));
SELECT a, r, toTypeName(r) FROM row_mv_gate;
DROP TABLE row_mv_gate;

-- A materialized view writing into an existing table stores nothing of its own: the target table was
-- gated when it was created, so the view itself stays creatable without the opt-in.
CREATE TABLE row_mv_gate_dst (a UInt64, r Row(x UInt64, y String)) ENGINE = MergeTree ORDER BY a;
SET allow_experimental_row_type = 0;
CREATE MATERIALIZED VIEW row_mv_gate TO row_mv_gate_dst AS SELECT a, r FROM row_mv_gate_src;

SET allow_experimental_row_type = 1;
INSERT INTO row_mv_gate_src VALUES (2, (2, 'yy'));
SELECT a, r, toTypeName(r) FROM row_mv_gate_dst;

DROP TABLE row_mv_gate;
DROP TABLE row_mv_gate_dst;
DROP TABLE row_mv_gate_src;
