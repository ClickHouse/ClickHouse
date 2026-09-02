-- A `CHECK` constraint is evaluated per block and read by block row, so an `arrayJoin` inside it checks a
-- row against another row's value, or - when an array is empty and the column ends up shorter than the
-- block - past the end of it. It is rejected at DDL time, like it already is for keys and indexes.

DROP TABLE IF EXISTS t_check_array_join;

CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK arrayJoin(arr) > 0) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }
CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK unnest(arr) > 0) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK length(arr) > 0) ENGINE = MergeTree ORDER BY k;
SELECT 'accepted';

ALTER TABLE t_check_array_join ADD CONSTRAINT c2 CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_check_array_join ADD CONSTRAINT c2 CHECK k < 1000;
SELECT 'altered';

INSERT INTO t_check_array_join VALUES (1, [1, 2]);
SELECT count() FROM t_check_array_join;

DROP TABLE t_check_array_join;
