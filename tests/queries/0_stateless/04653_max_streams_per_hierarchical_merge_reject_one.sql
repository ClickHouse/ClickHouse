-- `max_streams_per_hierarchical_merge = 1` must be rejected rather than silently coerced to 2.
-- A merge node with a single input does not reduce the number of streams, so no merge tree can be
-- built from it. Accepting the value and quietly using 2 would make `system.settings` disagree with
-- the pipeline that is actually built.

SELECT '-- assignment itself is accepted, the value is only rejected when a full-sort pipeline is built --';

SET max_streams_per_hierarchical_merge = 1;
SELECT getSetting('max_streams_per_hierarchical_merge');
-- No full sort here, so nothing validates the setting.
SELECT count() FROM numbers(10);

SELECT '-- rejected on the full sort path --';
SELECT number FROM numbers(10) ORDER BY number; -- { serverError BAD_ARGUMENTS }

SELECT '-- 0 and values >= 2 are accepted --';
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number SETTINGS max_streams_per_hierarchical_merge = 0);
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number SETTINGS max_streams_per_hierarchical_merge = 2);
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number SETTINGS max_streams_per_hierarchical_merge = 16);

SELECT '-- rejected on the serialized query plan path --';
-- The setting is not serialized and the deserializing side resets it to 0, so the rebuilt full sort
-- would never see the invalid value. It has to be rejected while the plan is serialized instead.
SELECT number FROM numbers(10) ORDER BY number
SETTINGS serialize_query_plan = 1, max_streams_per_hierarchical_merge = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- the same for a full sorting merge join --';
SELECT count() FROM numbers(10) AS l JOIN numbers(10) AS r ON l.number = r.number
SETTINGS serialize_query_plan = 1, join_algorithm = 'full_sorting_merge', max_streams_per_hierarchical_merge = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 0 and values >= 2 are accepted on the serialized query plan path --';
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number) SETTINGS serialize_query_plan = 1, max_streams_per_hierarchical_merge = 0;
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number) SETTINGS serialize_query_plan = 1, max_streams_per_hierarchical_merge = 16;
