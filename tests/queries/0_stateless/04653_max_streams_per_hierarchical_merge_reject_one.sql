-- `max_streams_per_hierarchical_merge = 1` must be rejected rather than silently coerced to 2.
-- A merge node with a single input does not reduce the number of streams, so no merge tree can be
-- built from it. Accepting the value and quietly using 2 would make `system.settings` disagree with
-- the pipeline that is actually built.

SELECT '-- assignment itself is accepted, the value is only rejected when a sorting plan is built --';

SET max_streams_per_hierarchical_merge = 1;
SELECT getSetting('max_streams_per_hierarchical_merge');
-- No `SortingStep` here, so nothing validates the setting.
SELECT count() FROM numbers(10);

SELECT '-- rejected on the full sort path --';
SELECT number FROM numbers(10) ORDER BY number; -- { serverError BAD_ARGUMENTS }

SELECT '-- rejected with serialize_query_plan as well --';
-- `SortingStep::Settings` has a second constructor taking `QueryPlanSerializationSettings`, which also
-- validates the value. That constructor cannot be reached with 1 from SQL: an initiator builds its own
-- plan (and therefore throws) before serializing anything, so it can never send 1 to a worker. This
-- case only pins down that enabling plan serialization does not bypass the check.
SELECT number FROM numbers(10) ORDER BY number SETTINGS serialize_query_plan = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- rejected on the read in order path --';
DROP TABLE IF EXISTS t_hm_reject;
CREATE TABLE t_hm_reject (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_hm_reject SELECT number FROM numbers(100);
INSERT INTO t_hm_reject SELECT number FROM numbers(100, 100);
SELECT a FROM t_hm_reject ORDER BY a SETTINGS optimize_read_in_order = 1, read_in_order_two_level_merge_threshold = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 0 and values >= 2 are accepted --';
SELECT count() FROM (SELECT a FROM t_hm_reject ORDER BY a SETTINGS max_streams_per_hierarchical_merge = 0);
SELECT count() FROM (SELECT a FROM t_hm_reject ORDER BY a SETTINGS max_streams_per_hierarchical_merge = 2);
SELECT count() FROM (SELECT a FROM t_hm_reject ORDER BY a SETTINGS max_streams_per_hierarchical_merge = 16);

DROP TABLE t_hm_reject;
