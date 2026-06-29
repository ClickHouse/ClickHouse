-- Tags: no-fasttest

SET enable_time_time64_type = 1;
SET session_timezone = 'UTC';

-- Time from numeric value (45045 = 12:30:45 as seconds since midnight)
SELECT 'Time from Int64 numeric';
SELECT JSONExtract('{"t": 45045}', 't', 'Time');

-- Time from numeric zero
SELECT 'Time from zero';
SELECT JSONExtract('{"t": 0}', 't', 'Time');

-- Time from string (already works, sanity check)
SELECT 'Time from string';
SELECT JSONExtract('{"t": "12:30:45"}', 't', 'Time');

-- Time64 from numeric value (already works, sanity check)
SELECT 'Time64 from Int64 numeric';
SELECT JSONExtract('{"t": 45045123}', 't', 'Time64(3)');

-- DateTime from numeric value (same bug as TimeNode)
SELECT 'DateTime from Int64 numeric';
SELECT JSONExtract('{"t": 1234567890}', 't', 'DateTime') SETTINGS date_time_output_format = 'iso';

-- DateTime from string (already works, sanity check)
SELECT 'DateTime from string';
SELECT JSONExtract('{"t": "2009-02-13 23:31:30"}', 't', 'DateTime') SETTINGS date_time_output_format = 'iso';

-- DateTime64 from numeric value (already works, sanity check)
SELECT 'DateTime64 from Int64 numeric';
SELECT JSONExtract('{"t": 1234567890123}', 't', 'DateTime64(3)') SETTINGS date_time_output_format = 'iso';

-- Large UInt64 value that is genuinely UInt64 (> INT64_MAX) for DateTime
SELECT 'DateTime from large UInt64';
SELECT JSONExtract('{"t": 4294967295}', 't', 'DateTime') SETTINGS date_time_output_format = 'iso';

-- DateTime from Int64
SELECT 'DateTime from Int64';
SELECT JSONExtract('{"t": 1}', 't', 'DateTime');
SELECT JSONExtract('{"t": 1234567890}', 't', 'DateTime');
SELECT JSONExtract('{"t": -1}', 't', 'DateTime');
SELECT JSONExtract('{"t": -1234567890}', 't', 'DateTime');
