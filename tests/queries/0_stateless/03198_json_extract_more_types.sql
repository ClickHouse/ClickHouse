set allow_suspicious_low_cardinality_types=1;

select JSONExtract('{"a" : "2020-01-01"}', 'a', 'Date');
select JSONExtract('{"a" : "2020-01-01"}', 'a', 'Date32');
select JSONExtract('{"a" : "2020-01-01 00:00:00"}', 'a', 'DateTime');
select JSONExtract('{"a" : "2020-01-01 00:00:00.000000"}', 'a', 'DateTime64(6)');
select JSONExtract('{"a" : "127.0.0.1"}', 'a', 'IPv4');
select JSONExtract('{"a" : "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}', 'a', 'IPv6');


select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(UInt8)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(Int8)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(UInt16)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(Int16)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(UInt32)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(Int32)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(UInt64)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(Int64)');

select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(Float32)');
select JSONExtract('{"a" : 42}', 'a', 'LowCardinality(Float32)');

select JSONExtract('{"a" : "Hello"}', 'a', 'LowCardinality(String)');
select JSONExtract('{"a" : "Hello"}', 'a', 'LowCardinality(FixedString(5))');
select JSONExtract('{"a" : "Hello"}', 'a', 'LowCardinality(FixedString(3))');
select JSONExtract('{"a" : "Hello"}', 'a', 'LowCardinality(FixedString(10))');

select JSONExtract('{"a" : "5801c962-1182-458a-89f8-d077da5074f9"}', 'a', 'LowCardinality(UUID)');

