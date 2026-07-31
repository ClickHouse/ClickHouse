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
