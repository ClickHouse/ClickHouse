-- Tags: no-parallel-replicas
-- no-parallel-replicas: read_bytes in the initiator's query_log does not include the replicas' reads.

-- A condition on a cheap subcolumn gets its own PREWHERE read step, so the column it is combined with is read
-- only for the rows it keeps. No row passes the first condition, so a case reads about the bytes of that one alone.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET move_all_conditions_to_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_maps;

CREATE TABLE t_json (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_tuple (id UInt64, t Tuple(kind String, payload String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
-- Bucketed, with as many buckets as keys, so that `m['k1']` reads one bucket and not the whole Map.
CREATE TABLE t_map (id UInt64, m Map(String, String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', max_buckets_in_map = 16, map_buckets_min_avg_size = 0;
CREATE TABLE t_maps (id UInt64, c Tuple(left Map(String, String), right Map(String, String))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Random values, so that the size of a column on disk, by which the conditions are ordered, reflects the cost of reading it.
INSERT INTO t_json SELECT number, concat('{"kind":"a","payload":"', hex(randomString(500)), '"}') FROM numbers(20000);
INSERT INTO t_tuple SELECT number, tuple('a', hex(randomString(500))) FROM numbers(20000);
INSERT INTO t_map SELECT number, mapFromArrays(arrayMap(i -> 'k' || toString(i), range(16)), arrayMap(i -> hex(randomString(30)), range(16))) FROM numbers(20000);
INSERT INTO t_maps SELECT number, tuple(map('k', 'a'), map('k', hex(randomString(500)))) FROM numbers(20000);

SELECT count() FROM t_json WHERE j.kind = 'b' FORMAT Null SETTINGS log_comment = 'json_cheap';
SELECT count() FROM t_tuple WHERE t.kind = 'b' FORMAT Null SETTINGS log_comment = 'tuple_cheap';
SELECT count() FROM t_map WHERE length(m) = 3 FORMAT Null SETTINGS log_comment = 'map_cheap';
SELECT count() FROM t_map WHERE m['k1'] = 'x' FORMAT Null SETTINGS log_comment = 'map_key_cheap';
SELECT count() FROM t_maps WHERE c.left['k'] = 'b' FORMAT Null SETTINGS log_comment = 'maps_cheap';

SELECT count() FROM t_json WHERE j.kind = 'b' AND length(toString(j)) > 10 FORMAT Null SETTINGS log_comment = 'json_whole';
SELECT count() FROM t_json PREWHERE j.kind = 'b' AND length(toString(j)) > 10 FORMAT Null SETTINGS log_comment = 'json_whole_prewhere';
SELECT count() FROM t_json WHERE j.kind = 'b' AND length(toString(j.payload)) > 10 FORMAT Null SETTINGS log_comment = 'json_other_path';
SELECT count() FROM t_tuple WHERE t.kind = 'b' AND length(toString(t)) > 10 FORMAT Null SETTINGS log_comment = 'tuple_whole';
-- PREWHERE keeps the written order, which the WHERE optimizer may change based on the column sizes.
SELECT count() FROM t_map PREWHERE length(m) = 3 AND m['k1'] != '' FORMAT Null SETTINGS log_comment = 'map_size_and_key';
SELECT count() FROM t_maps WHERE c.left['k'] = 'b' AND c.right['k'] != '' FORMAT Null SETTINGS log_comment = 'maps_other_map';
SELECT count() FROM t_map WHERE m['k1'] = 'x' AND length(toString(m)) > 10 FORMAT Null SETTINGS log_comment = 'map_key_and_whole';

SYSTEM FLUSH LOGS query_log;

SELECT c.case_name, q.bytes < 2 * base.bytes
FROM VALUES('idx UInt8, case_name String, base_name String',
    (1, 'json_whole', 'json_cheap'),
    (2, 'json_whole_prewhere', 'json_cheap'),
    (3, 'json_other_path', 'json_cheap'),
    (4, 'tuple_whole', 'tuple_cheap'),
    (5, 'map_size_and_key', 'map_cheap'),
    (6, 'maps_other_map', 'maps_cheap'),
    (7, 'map_key_and_whole', 'map_key_cheap')) AS c
INNER JOIN
(
    SELECT log_comment, max(read_bytes) AS bytes FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' GROUP BY log_comment
) AS q ON q.log_comment = c.case_name
INNER JOIN
(
    SELECT log_comment, max(read_bytes) AS bytes FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' GROUP BY log_comment
) AS base ON base.log_comment = c.base_name
ORDER BY c.idx;

DROP TABLE t_json;
DROP TABLE t_tuple;
DROP TABLE t_map;
DROP TABLE t_maps;
