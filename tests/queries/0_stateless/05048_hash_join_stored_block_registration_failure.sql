-- Tags: no-fasttest, no-parallel
-- no-fasttest: needs a build with libfiu to enable the failpoint.
-- no-parallel: the failpoint is server-wide and fires once, so a concurrent copy of this test
-- consumes the trigger and the first insert below then succeeds.

DROP TABLE IF EXISTS t_join_registration;
CREATE TABLE t_join_registration (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);

SYSTEM ENABLE FAILPOINT stored_columns_index_throw_on_add;
INSERT INTO t_join_registration VALUES (1, 1); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT stored_columns_index_throw_on_add;

-- A failed insert must leave nothing behind: the next insert re-checks that `data->columns` and
-- `data->allocated_size` agree.
INSERT INTO t_join_registration VALUES (2, 2);
SELECT k, v FROM t_join_registration ORDER BY k;

DROP TABLE t_join_registration;
