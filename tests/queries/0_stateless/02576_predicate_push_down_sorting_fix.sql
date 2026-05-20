SET explain_query_plan_default = "legacy";
SET enable_analyzer = 1;

EXPLAIN header = 1, actions = 1 SELECT number FROM (SELECT number FROM numbers(2) ORDER BY ignore(2)) WHERE ignore(2);
