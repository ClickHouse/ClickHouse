CREATE TABLE t0 (c0 Int32) ENGINE = GenerateRandom(rand()); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t1 (c0 Int32) ENGINE = GenerateRandom(now() % 1073741824);
CREATE TABLE t2 (c0 Int32) ENGINE = GenerateRandom(1 + 1);
CREATE TABLE t4 (c0 Int32) ENGINE = GenerateRandom(now() % 1073741824, 1+1, '123'::UInt64);
