-- https://github.com/ClickHouse/ClickHouse/issues/115448
-- `allow_statistics` gates the DDL - whether statistics may be declared - and only the submitting
-- session's value is meaningful for that. It used to be checked again while the mutation ran, under
-- the background context that reads the server-default profile, so a `MATERIALIZE STATISTICS`
-- accepted with a session opt-in failed forever and wedged the table's mutation queue. The full
-- reproducer needs a server profile with `allow_statistics = 0`, which a stateless test cannot set;
-- what is pinned here is that submission-time validation still refuses the statement, and that the
-- statement works with the setting on.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_materialize_statistics_gate;
CREATE TABLE t_materialize_statistics_gate (k UInt64, a Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_materialize_statistics_gate SELECT number, number FROM numbers(100);

SELECT 'refused at submission';
ALTER TABLE t_materialize_statistics_gate ADD STATISTICS a TYPE tdigest SETTINGS allow_statistics = 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_materialize_statistics_gate MATERIALIZE STATISTICS a SETTINGS allow_statistics = 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_materialize_statistics_gate DROP STATISTICS a SETTINGS allow_statistics = 0; -- { serverError INCORRECT_QUERY }

SELECT 'accepted and finishes';
ALTER TABLE t_materialize_statistics_gate ADD STATISTICS a TYPE tdigest SETTINGS allow_statistics = 1;
ALTER TABLE t_materialize_statistics_gate MATERIALIZE STATISTICS a SETTINGS allow_statistics = 1;
SELECT countIf(is_done = 0) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_materialize_statistics_gate';
SELECT count() FROM t_materialize_statistics_gate;

ALTER TABLE t_materialize_statistics_gate DROP STATISTICS a SETTINGS allow_statistics = 1;
SELECT countIf(is_done = 0) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_materialize_statistics_gate';

DROP TABLE t_materialize_statistics_gate;
