-- Tags: no-fasttest

SET allow_experimental_object_type = 1;
SET output_format_json_escape_forward_slashes = 0;

DROP TABLE IF EXISTS t_json_extract;
DROP TABLE IF EXISTS mv_json_extract;

SELECT 'MATERIALIZED';

CREATE TABLE t_json_extract
(
    json Object('JSON'),
    id UInt64 MATERIALIZED JSONExtractUInt(json, 'id'),
    id_alias UInt64 ALIAS JSONExtractUInt(json, 'id'),
    url MATERIALIZED JSONExtractString(json, 'obj', 'domain') || '/' || JSONExtractString(json, 'obj', 'path'),
    url_alias ALIAS JSONExtractString(json, 'obj', 'domain') || '/' || JSONExtractString(json, 'obj', 'path')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_json_extract (json) VALUES ('{"id": 1, "obj": {"domain": "google.com", "path": "mail"}}');
INSERT INTO t_json_extract (json) VALUES ('{"id": 2, "obj": {"domain": "clickhouse.com", "user_id": 3222}}');

SELECT id, id_alias, url, url_alias, json FROM t_json_extract ORDER BY id FORMAT JSONEachRow;

DROP TABLE IF EXISTS t_json_extract;
DROP TABLE IF EXISTS mv_json_extract;

SELECT 'MATERIALIZED VIEW';

CREATE TABLE t_json_extract(json Object('JSON')) ENGINE = MergeTree ORDER BY tuple();

CREATE MATERIALIZED VIEW mv_json_extract ENGINE = MergeTree ORDER BY id AS
SELECT
    json,
    JSONExtractUInt(json, 'id') AS id,
    JSONExtractString(json, 'obj', 'domain') || '/' || JSONExtractString(json, 'obj', 'path') AS url
FROM t_json_extract;

SHOW CREATE TABLE mv_json_extract;

INSERT INTO t_json_extract (json) VALUES ('{"id": 1, "obj": {"domain": "google.com", "path": "mail"}}');
INSERT INTO t_json_extract (json) VALUES ('{"id": 2, "obj": {"domain": "clickhouse.com", "user_id": 3222}}');

SELECT json FROM t_json_extract ORDER BY json.id FORMAT JSONEachRow;
SELECT id, url, json FROM mv_json_extract ORDER BY id FORMAT JSONEachRow;

DROP TABLE IF EXISTS t_json_extract;
DROP TABLE IF EXISTS mv_json_extract;

SELECT 'ENGINE = Null';

CREATE TABLE t_json_extract(json Object('JSON')) ENGINE = Null;

CREATE MATERIALIZED VIEW mv_json_extract ENGINE = MergeTree ORDER BY id AS
SELECT
    json,
    JSONExtractUInt(json, 'id') AS id,
    JSONExtractString(json, 'obj', 'domain') || '/' || JSONExtractString(json, 'obj', 'path') AS url
FROM t_json_extract;

INSERT INTO t_json_extract (json) VALUES ('{"id": 1, "obj": {"domain": "google.com", "path": "mail"}}');
INSERT INTO t_json_extract (json) VALUES ('{"id": 2, "obj": {"domain": "clickhouse.com", "user_id": 3222}}');

SELECT id, url, json FROM mv_json_extract ORDER BY id FORMAT JSONEachRow;

DROP TABLE IF EXISTS t_json_extract;
DROP TABLE IF EXISTS mv_json_extract;
