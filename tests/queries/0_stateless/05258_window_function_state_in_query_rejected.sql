-- A query or a binary type header must not produce the state of a function that only works as a window function.
CREATE VIEW v_window_state AS SELECT rankState() OVER () AS s FROM numbers(1); -- { serverError BAD_ARGUMENTS }
CREATE VIEW v_window_simple_state AS SELECT rankSimpleState() OVER () AS s FROM numbers(1); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_window_state ENGINE = Memory EMPTY AS SELECT rankState() OVER () AS s FROM numbers(1); -- { serverError BAD_ARGUMENTS }
DESCRIBE format(Native, '\x01\x00\x01s\x25\x00\x04rank\x00\x00') SETTINGS input_format_native_decode_types_in_binary_format = 1, output_format_native_encode_types_in_binary_format = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
-- A query that never computes the state is refused as well.
SELECT toTypeName(rankState() OVER ()); -- { serverError BAD_ARGUMENTS }
EXPLAIN SELECT rankState() OVER () FROM numbers(1); -- { serverError BAD_ARGUMENTS }
SELECT number FROM (SELECT number, rankState() OVER () AS s FROM numbers(3)); -- { serverError BAD_ARGUMENTS }
SELECT initializeAggregation('lagInFrameState', number) FROM numbers(0); -- { serverError BAD_ARGUMENTS }
-- An ordinary aggregate function's state is still accepted by both.
DESCRIBE format(Native, '\x01\x00\x01s\x25\x00\x03sum\x00\x01\x04') SETTINGS input_format_native_decode_types_in_binary_format = 1, output_format_native_encode_types_in_binary_format = 1;
CREATE VIEW v_window_state_ok AS SELECT sumState(number) OVER () AS s FROM numbers(3);
SELECT finalizeAggregation(s) FROM v_window_state_ok;
DROP VIEW v_window_state_ok;
