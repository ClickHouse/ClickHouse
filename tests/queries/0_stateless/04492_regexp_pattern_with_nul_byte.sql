-- A NUL byte (\0) in a regular expression must be treated as an ordinary literal byte
-- (RE2 is binary-safe), not as the end of the pattern. Regression for the analyzer in
-- OptimizedRegularExpression, which used to stop at the first NUL and then let the strstr
-- pre-filter drop every row.

-- Leading `\0*` (zero or more NUL bytes) is optional, so the rest of the pattern must still match.
SELECT extract('test@clickhouse.com', '\0*@(.*)$');
SELECT match('test@clickhouse.com', '\0*@.*');
SELECT replaceRegexpAll('a@b@c', '\0*@', '#');
SELECT arrayStringConcat(extractAll('test@x.com', '\0*([a-z]+)'), ',');

-- A literal NUL in the pattern matches a literal NUL in the data, and only that.
SELECT match('xa\0by', 'a\0b');
SELECT match('xayb', 'a\0b');
SELECT hex(extract('za\0bz', '(a\0b)'));

-- Now that the analyzer reports captures placed after a NUL byte, `RegexpFunctionRewritePass`
-- (`optimize_rewrite_regexp_functions`, on by default) could see them and strip the `^.*` prefix
-- of an `extract` pattern. That rewrite changes which occurrence is captured for a multi-match
-- pattern, so it must be skipped for patterns containing a NUL. The greedy `^.*` selects the last
-- match, so the result must be `second` (not `first`). `materialize` prevents constant folding so
-- the optimizer pass actually runs on the function.
SELECT extract(materialize('x\0first y\0second'), '^.*\0([a-z]+).*$')
SETTINGS enable_analyzer = 1, optimize_rewrite_regexp_functions = 1;

-- A real NUL byte as a character-class range endpoint makes a descending range `[a-\0]`, which RE2
-- rejects at compile time. The regexp-JIT class parser must not mistake the NUL for the end of the
-- pattern and silently accept the class - even with compilation forced it must fall back to RE2 and
-- raise the same error, not turn the RE2 compile failure into a successful match.
SELECT match('-', '[a-\0]')
SETTINGS compile_regular_expressions = 1, min_count_to_compile_regular_expression = 0; -- { serverError CANNOT_COMPILE_REGEXP }
