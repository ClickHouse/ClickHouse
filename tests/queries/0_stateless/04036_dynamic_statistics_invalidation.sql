-- Tags: no-random-settings

SET allow_experimental_dynamic_type=1;
SET allow_suspicious_types_in_order_by=1;

DROP TABLE IF EXISTS t_dyn_repro;
DROP TABLE IF EXISTS t2_dyn_repro;

CREATE TABLE t_dyn_repro (d Dynamic(max_types=254)) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t_dyn_repro VALUES (1::Int8), (2::Int8);
INSERT INTO t_dyn_repro VALUES (1::UInt32), (2::UInt32);
INSERT INTO t_dyn_repro VALUES ('hello'::String);
INSERT INTO t_dyn_repro VALUES (1.5::Float32);

CREATE TABLE t2_dyn_repro (d Dynamic(max_types=254)) ENGINE = MergeTree() ORDER BY tuple();

-- This INSERT reads from multiple parts with different variant types.
-- Previously it caused an exception because squashing extended variant_info
-- without resetting cached statistics, leading to stale statistics
-- in serializeBinaryBulkStatePrefix.
INSERT INTO t2_dyn_repro SELECT * FROM t_dyn_repro;

SELECT dynamicType(d), d FROM t2_dyn_repro ORDER BY d;

DROP TABLE t_dyn_repro;
DROP TABLE t2_dyn_repro;
