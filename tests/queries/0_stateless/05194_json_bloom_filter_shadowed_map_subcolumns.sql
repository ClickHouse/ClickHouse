SET use_skip_indexes = 1;

DROP TABLE IF EXISTS jsonbf_shadowed_column;
CREATE TABLE jsonbf_shadowed_column
(
    j JSON(m Map(String, String)),
    `j.m.key_nokey` String,
    INDEX idx j TYPE jsonbf_v1 GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO jsonbf_shadowed_column VALUES ('{"m":{"abc":"x"}}', 'hello');

SELECT 'physical column', count() FROM jsonbf_shadowed_column WHERE `j.m.key_nokey` = 'hello';
SELECT 'physical column, no index', count() FROM jsonbf_shadowed_column WHERE `j.m.key_nokey` = 'hello' SETTINGS use_skip_indexes = 0;
SELECT 'physical column, IN', count() FROM jsonbf_shadowed_column WHERE `j.m.key_nokey` IN ('hello', 'other');

DROP TABLE IF EXISTS jsonbf_shadowed_path;
CREATE TABLE jsonbf_shadowed_path
(
    j JSON(m Map(String, String), `m.key_nokey` String),
    INDEX idx j TYPE jsonbf_v1 GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO jsonbf_shadowed_path VALUES ('{"m.key_nokey":"hello"}');

SELECT 'typed JSON path', count() FROM jsonbf_shadowed_path WHERE j.`m.key_nokey` = 'hello';
SELECT 'typed JSON path, no index', count() FROM jsonbf_shadowed_path WHERE j.`m.key_nokey` = 'hello' SETTINGS use_skip_indexes = 0;
SELECT 'typed JSON path, IN', count() FROM jsonbf_shadowed_path WHERE j.`m.key_nokey` IN ('hello', 'other');

SELECT 'genuine map key still prunes',
       countIf(trim(explain) = 'Name: idx'),
       countIf(trim(explain) = 'Granules: 0/1')
FROM (EXPLAIN indexes = 1 SELECT count() FROM jsonbf_shadowed_path WHERE j.m.key_zzz = 'missing')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0, use_skip_indexes = 1;

DROP TABLE jsonbf_shadowed_column;
DROP TABLE jsonbf_shadowed_path;
