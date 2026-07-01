-- Tags: no-fasttest

-- Row policies applied by `vectorSearch` must be recorded in the query access
-- info, so that `system.query_log.used_row_policies` is populated the same way
-- as for a normal SELECT from the source table.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_rp_log;

CREATE TABLE tab_rp_log(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_rp_log VALUES (0, [0.0, 0.0]), (1, [1.0, 0.0]), (2, [2.0, 0.0]), (3, [3.0, 0.0]), (4, [4.0, 0.0]);

DROP ROW POLICY IF EXISTS policy_04355 ON tab_rp_log;
DROP ROW POLICY IF EXISTS policy_04355_restrictive ON tab_rp_log;
CREATE ROW POLICY policy_04355 ON tab_rp_log FOR SELECT USING id % 2 = 0 AS permissive TO CURRENT_USER;
CREATE ROW POLICY policy_04355_restrictive ON tab_rp_log FOR SELECT USING id < 4 AS restrictive TO CURRENT_USER;

SELECT '-- the policies are enforced';
SELECT id FROM vectorSearch(currentDatabase(), tab_rp_log, idx, [0.0, 0.0], 5) ORDER BY _score ASC, id;

SYSTEM FLUSH LOGS query_log;

SELECT '-- the policies are recorded in used_row_policies';
SELECT arraySort(used_row_policies)
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%FROM vectorSearch(%tab_rp_log%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds;

DROP ROW POLICY policy_04355 ON tab_rp_log;
DROP ROW POLICY policy_04355_restrictive ON tab_rp_log;
DROP TABLE tab_rp_log;
