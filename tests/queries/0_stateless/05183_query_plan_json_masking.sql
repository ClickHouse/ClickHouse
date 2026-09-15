-- Tags: no-old-analyzer

-- `system.query_log.query` is written from `query_for_logging`, which has already been through the
-- configured `query_masking_rules`. The plan lands on the same row and carries the same literals --
-- a filter's constants reach `Details` -- so it has to be masked too, or turning on
-- `log_query_plans` quietly defeats the masking an operator is relying on.
--
-- The rule used here is the `TOPSECRET.TOPSECRET` one the test server is configured with, in
-- tests/config/config.d/query_masking_rules.xml.

SET log_query_plans = 1;
SELECT count() FROM numbers(10) WHERE toString(number) = 'TOPSECRET.TOPSECRET' SETTINGS log_comment = '05183_masking' FORMAT Null;
SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    'masking',
    count(),
    -- The plan was captured at all, so the assertions below are about a real document.
    anyLast(isValidJSON(toJSONString(query_plan))),
    anyLast(length(JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes'))) > 0,
    -- Masked in the query text, as it always was.
    anyLast(position(query, 'TOPSECRET')) = 0,
    -- And masked in the plan, which is the point of this test.
    anyLast(position(toJSONString(query_plan), 'TOPSECRET')) = 0,
    -- The replacement really is present, so the literal was rewritten rather than dropped along
    -- with the whole step description.
    anyLast(position(toJSONString(query_plan), '[hidden]')) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05183_masking';
