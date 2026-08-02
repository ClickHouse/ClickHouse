SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

WITH t AS MATERIALIZED (SELECT abs(rand(42)) >= 0 AS x)
SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t;

CREATE TABLE test_table(repo_name String, event_type String, actor_login String) ENGINE = MergeTree() ORDER BY ();

EXPLAIN
WITH alexey_events AS MATERIALIZED
(
    SELECT repo_name, count() AS c FROM test_table
    WHERE event_type IN ('IssuesEvent', 'IssueCommentEvent', 'PullRequestEvent') AND actor_login = 'alexey-milovidov'
    GROUP BY repo_name ORDER BY count() DESC
    SETTINGS use_query_condition_cache = 0
)
SELECT * FROM alexey_events ORDER BY c DESC LIMIT 1
UNION ALL
SELECT * FROM alexey_events WHERE c = (SELECT medianExactDistinct(c) FROM alexey_events)
UNION ALL
SELECT * FROM alexey_events WHERE repo_name NOT ILIKE '%ClickHouse%' AND c = (SELECT medianExactDistinct(c) FROM alexey_events WHERE repo_name NOT ILIKE '%ClickHouse%');
