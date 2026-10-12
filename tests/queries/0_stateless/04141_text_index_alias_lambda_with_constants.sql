-- Regression test: text index on an ALIAS column whose expression contains a
-- lambda with captured constants must be usable in queries.
--
-- The lambda body materializes captured constants as a constant `ColumnFunction`
-- (e.g. `arrayMap((k, v) -> concat(k, '=', v), ...)` captures `'='`). The new
-- analyzer's column name for that constant carries an `_String` suffix and the
-- whole lambda is rendered as `k String, v String -> ...`, while the index
-- `sample_block` (built via the old analyzer) uses the AST form
-- `lambda(tuple(k, v), ...)` with bare literals. Without the fix
-- `header.has(...)` failed and the index was not used.

DROP TABLE IF EXISTS user_favorites;

CREATE TABLE user_favorites
(
    user_id UInt64,
    colors String,
    color_map Map(String, String),
    colors_text Array(String) ALIAS splitByChar(',', colors),
    colors_kv Array(String) ALIAS arrayMap((k, v) -> concat(k, '=', v), mapKeys(color_map), mapValues(color_map)),
    INDEX idx_colors_text colors_text TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_colors_kv_text colors_kv TYPE text(tokenizer = 'array') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO user_favorites VALUES (1, 'favorite=red,second=blue', {'favorite': 'red', 'second': 'blue'});
INSERT INTO user_favorites VALUES (2, 'favorite=green,favorite=blue', {'favorite': 'green', 'second': 'blue'});

-- The simple alias works (no lambda, no constants captured).
SELECT 'splitByChar', user_id FROM user_favorites WHERE has(colors_text, 'favorite=red')
SETTINGS force_data_skipping_indices = 'idx_colors_text';

-- The arrayMap alias used to be broken because of the captured `'='` constant.
SELECT 'arrayMap', user_id FROM user_favorites WHERE has(colors_kv, 'favorite=red')
SETTINGS force_data_skipping_indices = 'idx_colors_kv_text';

DROP TABLE user_favorites;

-- Minimal reproducer: a lambda body that captures a constant breaks index matching
-- whenever the lambda is folded into a constant `ColumnFunction`.
DROP TABLE IF EXISTS t_arr;

CREATE TABLE t_arr
(
    id UInt64,
    arr Array(String),
    arr_prefixed Array(String) ALIAS arrayMap(s -> concat('-', s), arr),
    INDEX idx_prefixed arr_prefixed TYPE text(tokenizer = 'array') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_arr VALUES (1, ['hello', 'world']);

SELECT 'concat-prefix', id FROM t_arr WHERE has(arr_prefixed, '-hello')
SETTINGS force_data_skipping_indices = 'idx_prefixed';

DROP TABLE t_arr;

-- Same column-name-matching path is taken by `bloom_filter`-family indices, so
-- guard them too: with the same captured-constant lambda an `ALIAS` column
-- bound to a `bloom_filter` index must still be selectable.
DROP TABLE IF EXISTS t_bf;

CREATE TABLE t_bf
(
    user_id UInt64,
    color_map Map(String, String),
    colors_kv Array(String) ALIAS arrayMap((k, v) -> concat(k, '=', v), mapKeys(color_map), mapValues(color_map)),
    INDEX idx_kv_bf colors_kv TYPE bloom_filter GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO t_bf VALUES (1, {'favorite': 'red', 'second': 'blue'});

SELECT 'bloom_filter arrayMap', user_id FROM t_bf WHERE has(colors_kv, 'favorite=red')
SETTINGS force_data_skipping_indices = 'idx_kv_bf';

DROP TABLE t_bf;

DROP TABLE IF EXISTS t_bf_arr;

CREATE TABLE t_bf_arr
(
    id UInt64,
    arr Array(String),
    arr_prefixed Array(String) ALIAS arrayMap(s -> concat('-', s), arr),
    INDEX idx_prefixed_bf arr_prefixed TYPE bloom_filter GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_bf_arr VALUES (1, ['hello', 'world']);

SELECT 'bloom_filter concat-prefix', id FROM t_bf_arr WHERE has(arr_prefixed, '-hello')
SETTINGS force_data_skipping_indices = 'idx_prefixed_bf';

DROP TABLE t_bf_arr;

-- Lambda argument names that require backquoting (contain whitespace, or clash
-- with SQL keywords) must produce identical column names on the AST and DAG
-- sides, so index matching keeps working.
DROP TABLE IF EXISTS t_quoted_lambda;

CREATE TABLE t_quoted_lambda
(
    user_id UInt64,
    color_map Map(String, String),
    colors_kv Array(String) ALIAS arrayMap((`my key`, `my value`) -> concat(`my key`, '=', `my value`), mapKeys(color_map), mapValues(color_map)),
    arr Array(String),
    arr_keyword Array(String) ALIAS arrayMap(`select` -> concat('-', `select`), arr),
    INDEX idx_kv_quoted colors_kv TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_kw_text arr_keyword TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_kw_bf arr_keyword TYPE bloom_filter GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO t_quoted_lambda VALUES (1, {'favorite': 'red', 'second': 'blue'}, ['hello', 'world']);

SELECT 'quoted lambda args', user_id FROM t_quoted_lambda WHERE has(colors_kv, 'favorite=red')
SETTINGS force_data_skipping_indices = 'idx_kv_quoted';

SELECT 'keyword lambda arg text', user_id FROM t_quoted_lambda WHERE has(arr_keyword, '-hello')
SETTINGS force_data_skipping_indices = 'idx_kw_text';

SELECT 'keyword lambda arg bloom_filter', user_id FROM t_quoted_lambda WHERE has(arr_keyword, '-hello')
SETTINGS force_data_skipping_indices = 'idx_kw_bf';

DROP TABLE t_quoted_lambda;

-- A lambda with more than three captured arguments and a captured-constant
-- separator (`concatWithSeparator('-', x1, x2, x3, x4)`) exercises the same
-- folding path with a wider capture list and a different higher-order function.
DROP TABLE IF EXISTS t_cws;

CREATE TABLE t_cws
(
    id UInt64,
    a Array(String),
    b Array(String),
    c Array(String),
    d Array(String),
    joined Array(String) ALIAS arrayMap((x1, x2, x3, x4) -> concatWithSeparator('-', x1, x2, x3, x4), a, b, c, d),
    INDEX idx_joined_text joined TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_joined_bf joined TYPE bloom_filter GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_cws VALUES (1, ['hello', 'foo'], ['world', 'bar'], ['a', 'baz'], ['b', 'qux']);

SELECT 'concatWithSeparator text', id FROM t_cws WHERE has(joined, 'hello-world-a-b')
SETTINGS force_data_skipping_indices = 'idx_joined_text';

SELECT 'concatWithSeparator bloom_filter', id FROM t_cws WHERE has(joined, 'hello-world-a-b')
SETTINGS force_data_skipping_indices = 'idx_joined_bf';

DROP TABLE t_cws;
