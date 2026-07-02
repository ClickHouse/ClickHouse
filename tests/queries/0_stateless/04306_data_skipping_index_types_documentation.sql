-- Data skipping index types expose embedded documentation via system.data_skipping_index_types.

-- Every registered index type must expose a non-empty description and syntax.
-- This query returns the offending rows, so the expected output is empty.
SELECT name
FROM system.data_skipping_index_types
WHERE length(description) = 0 OR length(syntax) = 0
ORDER BY name;

-- Core index types are present with documentation.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.data_skipping_index_types
WHERE name IN ('minmax', 'set', 'bloom_filter', 'tokenbf_v1', 'text')
ORDER BY name;

-- Related index types are exposed as an array.
SELECT related
FROM system.data_skipping_index_types
WHERE name = 'minmax';

-- The `sparse_grams` syntax must show the tokenizer arguments accepted by its validator
-- (`min_ngram_length`, `max_ngram_length`), not the obsolete single-`n` form.
SELECT
    syntax LIKE '%min_ngram_length%' AS has_min_ngram_length,
    syntax LIKE '%max_ngram_length%' AS has_max_ngram_length
FROM system.data_skipping_index_types
WHERE name = 'sparse_grams';

-- `vector_similarity` is only registered in builds with USearch; when present, its syntax
-- must include the required `dimensions` argument. Using an aggregate makes the result the
-- same (1) whether or not the type is registered in the current build.
SELECT countIf(syntax NOT LIKE '%dimensions%') = 0
FROM system.data_skipping_index_types
WHERE name = 'vector_similarity';
