CREATE TABLE destination_join ( `key` String, `id` String, `color` String, `section` String, `description` String) ENGINE = Join(ANY, LEFT, key);
CREATE TABLE destination_set (`key` String) ENGINE = Set;
CREATE MATERIALIZED VIEW mv_to_set TO `destination_set` AS SELECT key FROM destination_join;
INSERT INTO mv_to_set values ('kek');
