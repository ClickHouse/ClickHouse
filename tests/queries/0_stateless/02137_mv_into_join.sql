CREATE TABLE main ( `id` String, `color` String, `section` String, `description` String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE destination_join ( `key` String, `id` String, `color` String, `section` String, `description` String) ENGINE = Join(ANY, LEFT, key);
CREATE TABLE destination_set (`key` String) ENGINE = Set;

CREATE MATERIALIZED VIEW mv_to_join TO `destination_join` AS SELECT concat(id, '_', color) AS key, * FROM main;
CREATE MATERIALIZED VIEW mv_to_set TO `destination_set` AS SELECT key FROM destination_join;

INSERT INTO main VALUES ('sku_0001','black','women','nice shirt');
SELECT * FROM main;
SELECT * FROM destination_join;
SELECT * FROM destination_join WHERE key in destination_set;

DROP TABLE mv_to_set;
DROP TABLE destination_set;
DROP TABLE mv_to_join;
DROP TABLE destination_join;
DROP TABLE main;
