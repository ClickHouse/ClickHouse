SET session_timezone='UTC';

-- toDate32 function
CREATE TABLE test_date32_casts (from String, val Date32) Engine=Memory;

SELECT 'check date32 interpreted as seconds';
INSERT INTO test_date32_casts VALUES
    ('Int32', 120530::Int32),
    ('UInt32', 120530::UInt32),
    ('Int64', 120530::Int64),
    ('UInt64', 120530::UInt64),
    ('Int128', 120530::Int128),
    ('UInt128', 120530::UInt128),
    ('Int256', 120530::Int256),
    ('UInt256', 120530::UInt256),
    ('Float32', 120530::Float32),
    ('Float64', 120530::Float64),
    ('BFloat16', 121344::BFloat16); -- BFloat16 can't represent 120530 exactly, but it's still the same date
SELECT from, val, val::Int32 FROM test_date32_casts ORDER BY ALL;

TRUNCATE TABLE test_date32_casts;

SELECT 'check date32 interpreted as days';
INSERT INTO test_date32_casts VALUES
    ('Int32', 7::Int32),
    ('UInt32', 7::UInt32),
    ('Int64', 7::Int64),
    ('UInt64', 7::UInt64),
    ('Int128', 7::Int128),
    ('UInt128', 7::UInt128),
    ('Int256', 7::Int256),
    ('UInt256', 7::UInt256),
    ('Float32', 7::Float32),
    ('Float64', 7::Float64),
    ('BFloat16', 7::BFloat16);
SELECT from, val, val::Int32 FROM test_date32_casts ORDER BY ALL;

TRUNCATE TABLE test_date32_casts;

SELECT 'check date32 negative limit';
INSERT INTO test_date32_casts VALUES
    ('Int16', -30000::Int16),
    ('Int32', -30000::Int32),
    ('Int64', -30000::Int64),
    ('Int128', -30000::Int128),
    ('Int256', -30000::Int256),
    ('Float32', -30000::Float32),
    ('Float64', -30000::Float64),
    ('BFloat16', -30000::BFloat16);
SELECT from, val, val::Int32 FROM test_date32_casts ORDER BY ALL;

-- toDate function
CREATE TABLE test_date_casts (from String, val Date) Engine=Memory;

SELECT 'check date interpreted as seconds';
INSERT INTO test_date_casts VALUES
    ('Int32', 86400::Int32),
    ('UInt32', 86400::UInt32),
    ('Int64', 86400::Int64),
    ('UInt64', 86400::UInt64),
    ('Int128', 86400::Int128),
    ('UInt128', 86400::UInt128),
    ('Int256', 86400::Int256),
    ('UInt256', 86400::UInt256),
    ('Float32', 86400::Float32),
    ('Float64', 86400::Float64),
    ('BFloat16', 86800::BFloat16); -- BFloat16 can't represent 86400 exactly, but it's still the same date
SELECT from, val, val::UInt16 FROM test_date_casts ORDER BY ALL;

TRUNCATE TABLE test_date_casts;

SELECT 'check date interpreted as days';
INSERT INTO test_date_casts VALUES
    ('Int32', 7::Int32),
    ('UInt32', 7::UInt32),
    ('Int64', 7::Int64),
    ('UInt64', 7::UInt64),
    ('Int128', 7::Int128),
    ('UInt128', 7::UInt128),
    ('Int256', 7::Int256),
    ('UInt256', 7::UInt256),
    ('Float32', 7::Float32),
    ('Float64', 7::Float64),
    ('BFloat16', 7::BFloat16);
SELECT from, val, val::UInt16 FROM test_date_casts ORDER BY ALL;

TRUNCATE TABLE test_date_casts;

SELECT 'check date negative use zero';
INSERT INTO test_date_casts VALUES
    ('Int16', -10::Int8),
    ('Int16', -10::Int16),
    ('Int32', -10::Int32),
    ('Int64', -10::Int64),
    ('Int128', -10::Int128),
    ('Int256', -10::Int256),
    ('Float32', -10::Float32),
    ('Float64', -10::Float64),
    ('BFloat16', -10::BFloat16);
SELECT from, val, val::UInt16 FROM test_date_casts ORDER BY ALL;

-- Fuzzed
SELECT 'fuzz_71531';
CREATE TABLE fuzz_71531 (c0 Date32 DEFAULT -30000) ENGINE = Memory();
INSERT INTO TABLE fuzz_71531 (c0) VALUES (DEFAULT), ('2000-01-01');
SELECT c0 FROM fuzz_71531 ORDER BY c0 ASC;

SELECT 'fuzz_86799';
CREATE TABLE fuzz_86799 (c0 Date32) ENGINE = Memory;
INSERT INTO TABLE fuzz_86799 (c0) VALUES ('2000-01-01'), (799542731168215080 - 10::UInt128);
SELECT c0 FROM fuzz_86799 ORDER BY c0;
