-- `replaceRegexpOne` and `replaceRegexpAll` with a non-constant pattern reuse compiled patterns within a block.

-- Patterns with 0, 1 and 2 capturing groups and an empty one, each row compared with the constant-pattern result.
SELECT
    countIf(replaceRegexpAll(h, p, '<\\0>') != [replaceRegexpAll(h, '[aeiou]+', '<\\0>'), replaceRegexpAll(h, '(o)(r)?', '<\\0>'), replaceRegexpAll(h, 'W(o)', '<\\0>'), h][k + 1]),
    countIf(replaceRegexpOne(h, p, '<\\0>') != [replaceRegexpOne(h, '[aeiou]+', '<\\0>'), replaceRegexpOne(h, '(o)(r)?', '<\\0>'), replaceRegexpOne(h, 'W(o)', '<\\0>'), h][k + 1]),
    countIf(replaceRegexpAll(h, toLowCardinality(p), '<\\0>') != [replaceRegexpAll(h, '[aeiou]+', '<\\0>'), replaceRegexpAll(h, '(o)(r)?', '<\\0>'), replaceRegexpAll(h, 'W(o)', '<\\0>'), h][k + 1]),
    countIf(replaceRegexpAll(h, p, ['_', '\\2\\1', '[\\1]', 'x'][k + 1]) != [replaceRegexpAll(h, '[aeiou]+', '_'), replaceRegexpAll(h, '(o)(r)?', '\\2\\1'), replaceRegexpAll(h, 'W(o)', '[\\1]'), h][k + 1])
FROM (SELECT number % 4 AS k, concat('Hello World ', toString(number)) AS h, ['[aeiou]+', '(o)(r)?', 'W(o)', ''][k + 1] AS p FROM numbers(3000));

-- Two sets of 300 patterns, each seen twice: more patterns than buckets, and the second set lands in buckets
-- that hold compiled patterns of the first.
SELECT
    countIf(replaceRegexpAll(h, concat('^', h, '$'), 'Y') != 'Y'),
    countIf(replaceRegexpOne(h, concat('^', h, '$'), toString(number)) != toString(number))
FROM (SELECT number, concat('ax', toString(number % 300 + intDiv(number, 600) * 300), 'b') AS h FROM numbers(1200));

-- An invalid pattern fails after a valid one was cached, and a substitution fails for a pattern with fewer groups.
SELECT replaceRegexpAll(materialize('abc'), ['a', 'b', 'a', '('][number % 4 + 1], 'x') FROM numbers(8); -- { serverError BAD_ARGUMENTS }
SELECT replaceRegexpAll(materialize('abc'), ['(a)', 'b'][number % 2 + 1], '\\1') FROM numbers(4); -- { serverError BAD_ARGUMENTS }

SELECT sum(length(replaceRegexpOne(h, p, '_'))), sum(length(replaceRegexpAll(h, p, toString(number % 3))))
FROM (SELECT number, materialize('Hello World') AS h, ['[aeiou]+', '(o)(r)?', 'W(o)', 'l+'][number % 4 + 1] AS p FROM numbers(1000))
SETTINGS max_block_size = 1000, log_comment = '05241_replace_regexp_nonconst_pattern_cache';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RegexpLocalCacheMiss'], ProfileEvents['RegexpLocalCacheHit']
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05241_replace_regexp_nonconst_pattern_cache';
