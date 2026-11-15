-- Tags: no-fasttest
-- no-fasttest: the JSON type is not supported in the fast test build.

-- The shared data names a path's substreams from the `Dynamic` root, while the subcolumn used to
-- be read with a serialization numbering array sizes from the column root, so the reader asked the
-- substreams list for a name it does not hold.

DROP TABLE IF EXISTS t05213;
CREATE TABLE t05213 (jd Array(JSON(max_dynamic_paths=1))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS object_shared_data_serialization_version = 'advanced',
         object_shared_data_serialization_version_for_zero_level_parts = 'advanced';
INSERT INTO t05213 FORMAT JSONEachRow {"jd":[{"b":1,"zz":[[1,2],[3]]}]}
;

SELECT '-- fixture check: the path with the arrays must be in shared data';
SELECT JSONSharedDataPaths(jd[1]) FROM t05213;

SELECT '-- both array levels of a shared data path, read as subcolumns alone';
SELECT jd.zz.:`Array(Array(Nullable(Int64)))`.size1, jd.zz.:`Array(Array(Nullable(Int64)))`.size2 FROM t05213;

SELECT '-- the same sizes extracted in memory, by requesting the whole path alongside';
SELECT jd.zz, jd.zz.:`Array(Array(Nullable(Int64)))`.size1, jd.zz.:`Array(Array(Nullable(Int64)))`.size2 FROM t05213;

DROP TABLE t05213;
