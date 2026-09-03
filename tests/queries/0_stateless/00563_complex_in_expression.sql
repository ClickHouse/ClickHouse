DROP TABLE IF EXISTS test_00563;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE test_00563 ( dt Date, site_id Int32, site_key String ) ENGINE = MergeTree(dt, (site_id, site_key, dt), 8192);
INSERT INTO test_00563 (dt,site_id, site_key) VALUES ('2018-1-29', 100, 'key');
SELECT * FROM test_00563 WHERE toInt32(site_id) IN (100);
SELECT * FROM test_00563 WHERE toInt32(site_id) IN (100,101);

DROP TABLE IF EXISTS test_00563;

DROP TABLE IF EXISTS join_with_index;
CREATE TABLE join_with_index (key UInt32, data UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;
INSERT INTO join_with_index VALUES (1, 0), (2, 99);

SELECT key + 1
FROM join_with_index
ALL INNER JOIN
(
    SELECT
        key,
        data
    FROM join_with_index
    WHERE toUInt64(data) IN (0, 529335254087962442)
) js2 USING (key);

SELECT _uniq, _uniq IN (0, 99)
FROM join_with_index
ARRAY JOIN
    [key, data] AS _uniq
ORDER BY _uniq;

DROP TABLE IF EXISTS join_with_index;
