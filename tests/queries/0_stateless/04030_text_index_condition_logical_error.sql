CREATE TABLE tab__fuzz_44 (`text` LowCardinality(String), INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_experimental_reverse_key = 1, index_granularity = 64, lightweight_mutation_projection_mode = 'drop';
INSERT INTO tab__fuzz_44 VALUES ('foo');
SELECT count() IGNORE NULLS FROM tab__fuzz_44 PREWHERE equals(database, toUInt128(4, toUInt32(and(notEquals(7, 1), NULL, 1), 1, greater(7, materialize(1)), NULL, 1))) WHERE hasAllTokens(text, ['Hello']) FORMAT NULL SETTINGS allow_experimental_analyzer=1;
