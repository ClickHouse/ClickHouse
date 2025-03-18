DROP DATABASE IF EXISTS test_03393;
CREATE DATABASE test_03393 ENGINE = Atomic;
CREATE TABLE test_03393.target (c0 Int) ENGINE = Memory();

CREATE TABLE test_03393.err1 (c0 Int) ENGINE = Buffer(test_03393, target, 0, 1, 1, 1, 1, 1, 1); -- {serverError BAD_ARGUMENTS} must be a positive integer
CREATE TABLE test_03393.err0 (c0 Int) ENGINE = Buffer(test_03393, target, -1, 1, 1, 1, 1, 1, 1); -- {serverError BAD_ARGUMENTS} must be a literal with type UInt64
CREATE TABLE test_03393.err2 (c0 Int) ENGINE = Buffer(test_03393, target, 1, 1, -1, 1, 1, 1, 1); -- {serverError BAD_ARGUMENTS} must be a literal with type UInt64
CREATE TABLE test_03393.err3 (c0 Int) ENGINE = Buffer(test_03393, target, 1, 1, 1, -1, 1, 1, 1); -- {serverError BAD_ARGUMENTS} must be a literal with type UInt64
CREATE TABLE test_03393.err4 (c0 Int) ENGINE = Buffer(test_03393, target, 1, 1, 1, 1, -1, 1, 1); -- {serverError BAD_ARGUMENTS} must be a literal with type UInt64
CREATE TABLE test_03393.err5 (c0 Int) ENGINE = Buffer(test_03393, target, 1, 1, 1, 1, 1, -1, 1); -- {serverError BAD_ARGUMENTS} must be a literal with type UInt64
CREATE TABLE test_03393.err5 (c0 Int) ENGINE = Buffer(test_03393, target, 1, 1, 1, 1, 1, 1, -1); -- {serverError BAD_ARGUMENTS} must be a literal with type UInt64

SELECT name FROM system.tables WHERE database='test_03393';
