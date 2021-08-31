DROP DATABASE IF EXISTS 02015_db;
CREATE DATABASE IF NOT EXISTS 02015_db;

DROP TABLE IF EXISTS 02015_db.view_source_tb;
CREATE TABLE IF NOT EXISTS 02015_db.view_source_tb (a UInt8, s String) ENGINE = MergeTree() ORDER BY a;

DROP TABLE IF EXISTS 02015_db.materialized_view;
CREATE MATERIALIZED VIEW IF NOT EXISTS 02015_db.materialized_view ENGINE = ReplacingMergeTree() ORDER BY a AS SELECT * FROM 02015_db.view_source_tb;

SELECT * FROM system.views WHERE database='02015_db' and name = 'materialized_view';

DROP TABLE IF EXISTS 02015_db.materialized_view;
DROP TABLE IF EXISTS 02015_db.view_source_tb;
DROP DATABASE IF EXISTS 02015_db;
