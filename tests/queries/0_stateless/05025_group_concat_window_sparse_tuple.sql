-- A Tuple is never sparse itself, but its elements can be, so the columns that the window
-- functions read have to be materialized recursively.

DROP TABLE IF EXISTS t_group_concat_sparse_tuple;

CREATE TABLE t_group_concat_sparse_tuple (t Tuple(a String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.0;

INSERT INTO t_group_concat_sparse_tuple SELECT tuple(if(number % 200 = 0, 'y', '')) FROM numbers(4000);

SELECT length(groupConcat(t)) FROM t_group_concat_sparse_tuple;
SELECT DISTINCT length(groupConcat(t) OVER ()) FROM t_group_concat_sparse_tuple;

DROP TABLE t_group_concat_sparse_tuple;
