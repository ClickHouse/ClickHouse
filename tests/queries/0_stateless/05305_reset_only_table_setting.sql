-- Tags: use-rocksdb

DROP TABLE IF EXISTS t_reset_only_setting;
CREATE TABLE t_reset_only_setting (k UInt64, v UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k SETTINGS optimize_for_bulk_insert = 0;
ALTER TABLE t_reset_only_setting RESET SETTING optimize_for_bulk_insert;
SHOW CREATE TABLE t_reset_only_setting FORMAT TSVRaw;
DETACH TABLE t_reset_only_setting;
ATTACH TABLE t_reset_only_setting;
SHOW CREATE TABLE t_reset_only_setting FORMAT TSVRaw;
DROP TABLE t_reset_only_setting;
