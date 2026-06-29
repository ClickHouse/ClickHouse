-- Tags: no-fasttest, use-rocksdb

CREATE TABLE test (t Tuple(a Int32)) ENGINE = EmbeddedRocksDB() PRIMARY KEY (t.a); -- {serverError BAD_ARGUMENTS}

