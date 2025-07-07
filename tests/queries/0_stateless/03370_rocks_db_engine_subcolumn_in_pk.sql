-- Tags: no-fasttest

CREATE TABLE test (t Tuple(a Int32)) ENGINE = EmbeddedRocksDB() PRIMARY KEY (t.a); -- {serverError BAD_ARGUMENTS}

