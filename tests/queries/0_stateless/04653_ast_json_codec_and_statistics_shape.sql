-- The column `CODEC(...)`/`STATISTICS(...)` modifiers and the TTL `RECOMPRESS CODEC(...)` slot are
-- parser-owned shapes: `ParserCodec` and `ParserStatisticsType` always synthesize an `ASTFunction`
-- named `CODEC`/`STATISTICS`, with the matching kind and a non-empty argument list whose elements come
-- from `ParserIdentifierWithOptionalParameters` (hence functions as well). Consumers rely on exactly
-- that shape: `CompressionCodecFactory::validateCodecAndGetPreprocessedAST` dereferences
-- `arguments->children` and `ColumnStatisticsDescription::fromStatisticsDescriptionAST` downcasts each
-- entry to `ASTFunction`, so a malformed shape must be rejected at the JSON boundary instead of being
-- formatted as parser-impossible DDL and failing later.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(Delta, ZSTD(1)), b UInt64 STATISTICS(tdigest, uniq)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN a UInt64 CODEC(ZSTD(3))'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(ZSTD(6))'));

-- A `CODEC` function without arguments is parser-impossible and would be dereferenced by the codec factory.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"LZ4","no_empty_args":true}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- An empty codec list is parser-impossible too (`ParserList` there does not allow an empty list).
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"Function","name":"LZ4","no_empty_args":true}',
    '')); -- { serverError BAD_ARGUMENTS }

-- Every codec list element is a function, never a literal.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"Function","name":"LZ4","no_empty_args":true}',
    '{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- The function name and kind of the slot are fixed by the parser.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    '"codec":{"type":"Function","name":"CODEC"',
    '"codec":{"type":"Function","name":"NOT_CODEC"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'),
    ',"kind":"CODEC"',
    '')); -- { serverError BAD_ARGUMENTS }

-- The `codec` slot must be a function at all.
SELECT formatQueryFromJSON('{"type":"ColumnDeclaration","name":"a","ephemeral_default":false,"primary_key_specifier":false,"data_type":{"type":"DataType","name":"UInt64"},"codec":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }

-- `STATISTICS` entries are downcast to `ASTFunction`, so an identifier entry must be rejected.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"Function","name":"tdigest","no_empty_args":true}',
    '{"type":"Identifier","name":"tdigest"}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a'),
    '{"type":"Function","name":"tdigest","no_empty_args":true}',
    '')); -- { serverError BAD_ARGUMENTS }

-- The TTL `RECOMPRESS` slot carries the same parser-produced `CODEC(...)` shape.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(LZ4)'),
    '"recompression_codec":{"type":"Function","name":"CODEC","arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"LZ4","no_empty_args":true}]},"kind":"CODEC"}',
    '"recompression_codec":{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(LZ4)'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"LZ4","no_empty_args":true}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- Only `RECOMPRESS` carries a codec, because `formatImpl` drops it for every other TTL mode.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + toIntervalMonth(1) RECOMPRESS CODEC(LZ4)'),
    '"mode":"RECOMPRESS"',
    '"mode":"DELETE"')); -- { serverError BAD_ARGUMENTS }
