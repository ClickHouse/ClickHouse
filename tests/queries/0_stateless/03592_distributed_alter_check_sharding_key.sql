CREATE TABLE t1 (c0 Int, c1 Int) ENGINE = Distributed('test_shard_localhost', default, t0, `c1`);
ALTER TABLE t1 MODIFY COLUMN c1 String; -- { serverError TYPE_MISMATCH }
