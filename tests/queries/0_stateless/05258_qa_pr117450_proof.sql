-- Test hasPhrase with a LowCardinality tokenizer argument (ASan: dangling tokenizer-name string_view).
SELECT hasPhrase(materialize('a b c'), 'b c', 'splitByNonAlpha');
SELECT hasPhrase(materialize('a b c'), 'b c', toLowCardinality('splitByNonAlpha'));
SELECT hasPhrase(materialize('a b c'), 'b c', toLowCardinality('ngrams(2)'));
