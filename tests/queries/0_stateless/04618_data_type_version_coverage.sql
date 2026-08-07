-- Coverage for previously-untested Version usage: LowCardinality, Array, aggregate functions,
-- ORDER BY/primary key, PARTITION BY, toTypeName round-trip. If any assertion below fails, that
-- is a genuine bug uncovered by this test -- do not weaken the assertion to make it pass; flag it
-- clearly instead so it can be triaged and fixed properly.
-- A clean run of the throwIf() assertions below produces a single "0" line of output per
-- assertion (throwIf returns 0 rather than nothing when its condition is false).

SET allow_experimental_version_type = 1;

-- (1) LowCardinality(Version)
DROP TABLE IF EXISTS version_lc_test;
CREATE TABLE version_lc_test (id UInt32, ver LowCardinality(Version)) ENGINE = MergeTree ORDER BY id;
INSERT INTO version_lc_test VALUES (1, '1.2.3.4'), (2, '1.2.3.4'), (3, '2.0.0.0');
SELECT throwIf(toTypeName(ver) != 'LowCardinality(Version)', 'FAIL: LowCardinality(Version) type name') FROM version_lc_test LIMIT 1;
SELECT throwIf(count() != 2, 'FAIL: LowCardinality(Version) filter') FROM version_lc_test WHERE ver = toVersion('1.2.3.4');
DROP TABLE version_lc_test;

-- (2) Array(Version)
DROP TABLE IF EXISTS version_arr_test;
CREATE TABLE version_arr_test (id UInt32, vers Array(Version)) ENGINE = MergeTree ORDER BY id;
INSERT INTO version_arr_test VALUES (1, ['1.2.3.4', '2.0.0.0']);
SELECT throwIf(toTypeName(vers) != 'Array(Version)', 'FAIL: Array(Version) type name') FROM version_arr_test;
SELECT throwIf(length(vers) != 2, 'FAIL: Array(Version) length') FROM version_arr_test;
SELECT throwIf(vers[1] != toVersion('1.2.3.4'), 'FAIL: Array(Version) element access') FROM version_arr_test;
DROP TABLE version_arr_test;

-- (3) Aggregate functions: min, max, any, groupArray, uniq
DROP TABLE IF EXISTS version_agg_test;
CREATE TABLE version_agg_test (id UInt32, ver Version) ENGINE = MergeTree ORDER BY id;
INSERT INTO version_agg_test VALUES (1, '1.0.0.0'), (2, '2.0.0.0'), (3, '1.5.0.0');
SELECT throwIf(min(ver) != toVersion('1.0.0.0'), 'FAIL: min(Version)') FROM version_agg_test;
SELECT throwIf(max(ver) != toVersion('2.0.0.0'), 'FAIL: max(Version)') FROM version_agg_test;
SELECT throwIf(any(ver) IS NULL, 'FAIL: any(Version)') FROM version_agg_test;
SELECT throwIf(length(groupArray(ver)) != 3, 'FAIL: groupArray(Version)') FROM version_agg_test;
SELECT throwIf(uniq(ver) != 3, 'FAIL: uniq(Version)') FROM version_agg_test;
DROP TABLE version_agg_test;

-- (4) ORDER BY / primary key
DROP TABLE IF EXISTS version_orderby_test;
CREATE TABLE version_orderby_test (id UInt32, ver Version) ENGINE = MergeTree ORDER BY ver;
INSERT INTO version_orderby_test VALUES (3, '3.0.0.0'), (1, '1.0.0.0'), (2, '2.0.0.0');
SELECT throwIf(groupArray(id) != [1, 2, 3], 'FAIL: MergeTree ORDER BY Version does not read back sorted') FROM (SELECT id FROM version_orderby_test ORDER BY ver);
DROP TABLE version_orderby_test;

-- (5) PARTITION BY
DROP TABLE IF EXISTS version_partition_test;
CREATE TABLE version_partition_test (id UInt32, ver Version) ENGINE = MergeTree ORDER BY id PARTITION BY ver;
INSERT INTO version_partition_test VALUES (1, '1.0.0.0'), (2, '2.0.0.0'), (3, '1.0.0.0');
SELECT throwIf(uniqExact(_partition_id) != 2, 'FAIL: PARTITION BY Version did not create 2 distinct partitions') FROM version_partition_test;
DROP TABLE version_partition_test;

-- (6) toTypeName round-trip through a column (not just a bare scalar call)
DROP TABLE IF EXISTS version_typename_test;
CREATE TABLE version_typename_test (ver Version) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO version_typename_test VALUES ('1.2.3.4');
SELECT throwIf(toTypeName(ver) != 'Version', 'FAIL: toTypeName round-trip through column') FROM version_typename_test;
DROP TABLE version_typename_test;

SET allow_experimental_version_type = 0;
