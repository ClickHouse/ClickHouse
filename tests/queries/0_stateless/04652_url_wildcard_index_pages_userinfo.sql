-- The `URL` engine with a listable glob in the path is served by the object storage over HTTP index
-- pages. The resolved URL has to be materialized into the engine arguments, but it must never carry
-- credentials coming from `url_base` into the persisted `CREATE TABLE` query.

SET allow_experimental_url_wildcard_from_index_pages = 1;
-- Hive partitioning would list the object storage to sample a path, i.e. wait for the connection to
-- the unreachable host below to time out.
SET use_hive_partitioning = 0;

-- The host is unreachable on purpose: nothing below reads the data, only the persisted DDL is checked.
SET url_base = 'http://127.0.0.1:1/dir/';
CREATE TABLE url_wildcard_plain (x String) ENGINE = URL('sub/**/part*.tsv', 'TSV');

SET url_base = 'http://user:secretpassword@127.0.0.1:1/dir/';
CREATE TABLE url_wildcard_userinfo (x String) ENGINE = URL('sub/**/part*.tsv', 'TSV');

SELECT name, engine_full FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'url_wildcard_%'
ORDER BY name;

DROP TABLE url_wildcard_plain;
DROP TABLE url_wildcard_userinfo;
