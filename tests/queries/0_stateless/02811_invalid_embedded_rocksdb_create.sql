-- Tags: no-fasttest
CREATE TABLE dict (`k` String, `v` String) ENGINE = EmbeddedRocksDB(k) PRIMARY KEY k; -- {serverError 36}
