-- A column default with an aliased tuple element index gives the same value whether the server or the client fills it
DROP TABLE IF EXISTS t_tuple_index_alias_default;
SET async_insert = 0, send_table_structure_on_insert_with_inline_data = 1, input_format_defaults_for_omitted_fields = 1;

DROP TABLE IF EXISTS t_tuple_index_alias_default;
CREATE TABLE t_tuple_index_alias_default (k UInt8, c Tuple(Int32, Int32), d Int32 DEFAULT c.(1 AS a) + a) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_tuple_index_alias_default (k, c) SELECT 1, (5, 6);
-- `d` is omitted, so the client computes it
INSERT INTO t_tuple_index_alias_default FORMAT JSONEachRow {"k": 2, "c": [5, 6]};

SELECT k, d FROM t_tuple_index_alias_default ORDER BY k;
DROP TABLE t_tuple_index_alias_default;
