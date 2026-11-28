DROP TABLE IF EXISTS kv_baseline;

CREATE TABLE kv_baseline
(
    key String,
    value String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key;
