-- `ALTER TABLE ... MODIFY SETTING id_generator` of a `TimeSeries` table must materialize `uuid_type_version`
-- into the types named in the expression, the same way as `CREATE` does.

-- The `id_type` and `id_generator` settings apply to tables of version 9 or earlier only.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS uuid2_ts_alter_v1;
DROP TABLE IF EXISTS uuid2_ts_alter_v2;

SET uuid_type_version = 1;
CREATE TABLE uuid2_ts_alter_v1 ENGINE = TimeSeries SETTINGS version = 9, id_type = 'UUID', id_generator = 'reinterpretAsUUID(sipHash128(metric_name, all_tags))';
ALTER TABLE uuid2_ts_alter_v1 MODIFY SETTING id_generator = 'CAST(reinterpretAsUUID(sipHash128(metric_name, all_tags)), \'UUID\')';

SET uuid_type_version = 2;
CREATE TABLE uuid2_ts_alter_v2 ENGINE = TimeSeries SETTINGS version = 9, id_type = 'UUID', id_generator = 'reinterpretAsUUID(sipHash128(metric_name, all_tags))';
ALTER TABLE uuid2_ts_alter_v2 MODIFY SETTING id_generator = 'CAST(reinterpretAsUUID(sipHash128(metric_name, all_tags)), \'UUID\')';

SELECT name, extract(create_table_query, 'id_generator = (\'.*?\\)\')') FROM system.tables WHERE database = currentDatabase() AND name LIKE 'uuid2_ts_alter_v%' ORDER BY name FORMAT TSVRaw;

-- The definition is stable when the table is reattached under another setting.
SET uuid_type_version = 1;
DETACH TABLE uuid2_ts_alter_v2;
ATTACH TABLE uuid2_ts_alter_v2;
SELECT name, extract(create_table_query, 'id_generator = (\'.*?\\)\')') FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_ts_alter_v2' FORMAT TSVRaw;

DROP TABLE uuid2_ts_alter_v1;
DROP TABLE uuid2_ts_alter_v2;
