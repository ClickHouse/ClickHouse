-- The storage opens no connection at creation time, so the masking is checked without an Elasticsearch server.
SET enable_elasticsearch = 1;
SET max_threads = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS es_secret_positional;
DROP TABLE IF EXISTS es_secret_password;
DROP TABLE IF EXISTS es_secret_bearer;
DROP TABLE IF EXISTS es_secret_api_key;

CREATE TABLE es_secret_positional (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log');
CREATE TABLE es_secret_password (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', user = 'u', password = 'secret_value_do_not_log');
CREATE TABLE es_secret_bearer (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', bearer_token = 'secret_value_do_not_log');
CREATE TABLE es_secret_api_key (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', api_key = 'secret_value_do_not_log');

SELECT name, engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE 'es_secret_%' ORDER BY name;
SHOW CREATE TABLE es_secret_positional FORMAT TSVRaw;

DROP TABLE es_secret_positional;
DROP TABLE es_secret_password;
DROP TABLE es_secret_bearer;
DROP TABLE es_secret_api_key;

-- A failing query is still logged, and the secret must be masked in the log entry too.
SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log') SETTINGS http_max_tries = 1; -- { serverError POCO_EXCEPTION }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', api_key = 'secret_value_do_not_log') SETTINGS http_max_tries = 1; -- { serverError POCO_EXCEPTION }
SELECT * FROM elasticsearch(no_such_collection, password = 'secret_value_do_not_log'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

SYSTEM FLUSH LOGS query_log;
SELECT query FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND query LIKE '%elasticsearch(%'
  AND query NOT LIKE '%system.query_log%'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
ORDER BY event_time_microseconds
FORMAT TSVRaw;

-- { echoOn }
EXPLAIN AST SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log');
EXPLAIN SYNTAX SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log');
EXPLAIN QUERY TREE SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log');
SELECT sum(countSubstrings(explain, 'secret_value_do_not_log')) FROM (EXPLAIN actions = 1 SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log'));
SELECT sum(countSubstrings(explain, 'secret_value_do_not_log')) FROM (EXPLAIN PIPELINE SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log'));

-- The session setting alone must not reveal the secret: the stateless server has neither the
-- `display_secrets_in_show_and_select` server setting nor the `displaySecretsInShowAndSelect` privilege.
SET format_display_secrets_in_show_and_select = 1;
EXPLAIN QUERY TREE SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', 'secret_value_do_not_log');
-- { echoOff }
