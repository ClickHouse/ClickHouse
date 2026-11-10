-- Tags: no-fasttest
-- Reading a Protobuf row creates the aggregate state first; when `create` throws, the reader must not destroy that state.
SELECT length(finalizeAggregation(s)) FROM format(Protobuf, 's AggregateFunction(nothingResampleDistinctResample(0, 1048576, 1, 0, 3, 1), UInt64, UInt64)', '\x03\x0a\x01\x00') SETTINGS format_schema_source = 'string', format_schema = 'syntax = "proto3"; message M { bytes s = 1; }', format_schema_message_name = 'M'; -- { serverError ARGUMENT_OUT_OF_BOUND }
