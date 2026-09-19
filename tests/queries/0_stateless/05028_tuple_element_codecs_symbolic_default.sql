DROP TABLE IF EXISTS t_tuple_codec_symbolic_default;

SET enable_tuple_element_codecs = 1;

CREATE TABLE t_tuple_codec_symbolic_default
(
    payload Tuple(a Array(UInt64) CODEC(Delta, Default))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4';

DESCRIBE TABLE t_tuple_codec_symbolic_default
SETTINGS describe_include_subcolumns = 1
FORMAT JSONEachRow;

DROP TABLE t_tuple_codec_symbolic_default;
