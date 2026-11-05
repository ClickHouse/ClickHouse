-- Tags: atomic-database, no-random-settings
-- no-random-settings: the runner can pick `session_timezone=zoneinfo/UTC` as a connection-
-- level setting; the server then rejects every request (including any in-test `SET`) with
-- an unrelated `Invalid time zone: zoneinfo/UTC. (BAD_ARGUMENTS)` before the targeted check
-- can fire. An in-test `SET session_timezone = 'UTC'` does not help because URL/connection
-- settings are validated before the query is parsed.
-- Test: refreshable materialized view (without APPEND) must reject explicit `TO INNER UUID`.
-- Refresh in non-APPEND mode swaps the inner table on each refresh (different UUID each time),
-- so a user-fixed inner UUID is nonsensical. The check guards against silent confusion or
-- inconsistent UUIDs in the refresh task's EXCHANGE/DROP path.
-- Covers: src/Storages/StorageMaterializedView.cpp:263 -- if (to_inner_uuid != UUIDHelpers::Nil) throw BAD_ARGUMENTS
--         The branch fires only when refresh_strategy is set AND fixed_uuid (=APPEND) is false.
-- The CREATE below throws during storage construction *before* the inner UUID is registered
-- in the global UUID map, so a fixed literal UUID is safe under parallel reruns (no `no-parallel`).

DROP TABLE IF EXISTS src_for_refresh_uuid SYNC;
DROP TABLE IF EXISTS rmv_with_inner_uuid SYNC;

CREATE TABLE src_for_refresh_uuid (x Int64) ENGINE = Memory;
INSERT INTO src_for_refresh_uuid VALUES (1);

-- Non-APPEND REFRESH + TO INNER UUID: rejected.
CREATE MATERIALIZED VIEW rmv_with_inner_uuid
    REFRESH EVERY 1 YEAR
    TO INNER UUID '11111111-2222-3333-4444-555555555555'
    (x Int64) ENGINE = Memory
    AS SELECT x FROM src_for_refresh_uuid; -- { serverError BAD_ARGUMENTS }

SELECT 'rejected_without_append_ok';

DROP TABLE src_for_refresh_uuid SYNC;
