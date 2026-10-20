-- `CODEC(...)` is an `ASTCodec` and `STATISTICS(...)` an expression list, both holding
-- `NameWithParameters` elements and never empty; the codec factory and the statistics description
-- downcast them, so a malformed shape must be rejected at the JSON boundary instead of being
-- formatted as parser-impossible DDL and failing later.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(Delta, ZSTD(1)), b UInt64 STATISTICS(tdigest, uniq)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN a UInt64 CODEC(ZSTD(3))'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(ZSTD(6))'));

-- A codec without elements.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    ',"elements":{"type":"ExpressionList","children":[{"type":"NameWithParameters","name":"LZ4"}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- An empty codec list (`ParserList` there does not allow an empty list).
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"NameWithParameters","name":"LZ4"}',
    '')); -- { serverError BAD_ARGUMENTS }

-- Every codec element is a name with parameters, never a literal or a function.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"NameWithParameters","name":"LZ4"}',
    '{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"NameWithParameters","name":"LZ4"}',
    '{"type":"Function","name":"LZ4","no_empty_args":true}')); -- { serverError BAD_ARGUMENTS }

-- The `codec` slot must be a codec at all.
SELECT formatQueryFromJSON('{"type":"ColumnDeclaration","name":"a","ephemeral_default":false,"primary_key_specifier":false,"data_type":{"type":"DataType","name":"UInt64"},"codec":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }

-- `STATISTICS` entries are names with parameters too.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"NameWithParameters","name":"tdigest"}',
    '{"type":"Identifier","name":"tdigest"}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"NameWithParameters","name":"tdigest"}',
    '')); -- { serverError BAD_ARGUMENTS }

-- The TTL `RECOMPRESS` slot carries the same codec shape.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(LZ4)'),
    '"recompression_codec":{"type":"Codec","elements":{"type":"ExpressionList","children":[{"type":"NameWithParameters","name":"LZ4"}]}}',
    '"recompression_codec":{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(LZ4)'),
    ',"elements":{"type":"ExpressionList","children":[{"type":"NameWithParameters","name":"LZ4"}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- Only `RECOMPRESS` carries a codec, because `formatImpl` drops it for every other TTL mode.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(LZ4)'),
    '"mode":"RECOMPRESS"',
    '"mode":"DELETE"')); -- { serverError BAD_ARGUMENTS }
