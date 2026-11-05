-- Tags: no-fasttest
-- Test: exercises Time string parse failure path at JSONExtractTree.cpp:784
-- when an unparseable time string is passed to JSONExtract for Time type.
-- Covers: src/Formats/JSONExtractTree.cpp:784 — return false in !tryParse branch

SET enable_time_time64_type = 1;

-- Invalid time strings that fail parsing (error path at line 784)
SELECT 'Invalid Time strings - return default on parse failure';
SELECT JSONExtract('{"t": "not-a-time"}', 't', 'Time');
SELECT JSONExtract('{"t": "xyz"}', 't', 'Time');
SELECT JSONExtract('{"t": ""}', 't', 'Time');
SELECT JSONExtract('{"t": "abc:def:ghi"}', 't', 'Time');

-- Invalid DateTime strings that fail parsing (error path at line 713)
SELECT 'Invalid DateTime strings - return default on parse failure';
SELECT JSONExtract('{"t": "not-a-datetime"}', 't', 'DateTime(\'UTC\')');
SELECT JSONExtract('{"t": "bogus"}', 't', 'DateTime(\'UTC\')');
SELECT JSONExtract('{"t": ""}', 't', 'DateTime(\'UTC\')');
