DROP TABLE IF EXISTS mass_table_117;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE mass_table_117 (`dt` Date, `site_id` Int32, `site_key` String) ENGINE = MergeTree(dt, (site_id, site_key, dt), 8192);
INSERT INTO mass_table_117 SELECT * FROM generateRandom('`dt` Date,`site_id` Int32,`site_key` String', 1, 10, 2) LIMIT 100;
SELECT count(), sum(cityHash64(*)) FROM mass_table_117;
DROP TABLE mass_table_117;
