-- `optimize_or_like_chain` rewrites an `OR` chain of `LIKE`/`ILIKE` predicates into a single
-- `multiMatchAny`, which is executed by Vectorscan. Vectorscan follows PCRE semantics for the end
-- anchor: `$` matches at the end of the haystack *and* immediately before a final newline. The
-- original `like`/`ilike` matches the same regexp with re2 (or a byte-exact anchored-literal fast
-- path), where `$` means the absolute end of the string. `likePatternToRegexp` appends `$` to every
-- pattern that does not end in an unescaped `%`, so the rewrite used to match a *broader* set than
-- the query as written: `s LIKE 'tenant-a'` also returned `'tenant-a\n'`. That silently widens
-- LIKE-based filters, including filters that implement row filtering inside a `SQL SECURITY DEFINER`
-- view. End-anchored chains must therefore keep their original branches, and results must not depend
-- on `optimize_or_like_chain` or on `allow_hyperscan`.

SET optimize_or_like_chain_min_patterns = 1;

DROP TABLE IF EXISTS t_or_like_end_anchor;
CREATE TABLE t_or_like_end_anchor (s String) ENGINE = Memory;

-- Every value that ends in a newline is excluded by the patterns below, but is accepted by a PCRE `$`.
INSERT INTO t_or_like_end_anchor VALUES ('tenant-a'), ('tenant-a\n'), ('tenant-a\n\n'), ('x-tenant-a'), ('x-tenant-a\n'), ('taxb'), ('ta\nb'), ('ta\nb\n'), (''), ('\n'), ('a$'), ('a$\n');

-- Exact patterns: regexp `^tenant\-a$`.
SELECT 'exact / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant-a' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'exact / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant-a' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'exact / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant-a' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- Suffix patterns: regexp `tenant\-a$`.
SELECT 'suffix / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE '%tenant-a' OR s LIKE '%never' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'suffix / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE '%tenant-a' OR s LIKE '%never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'suffix / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE '%tenant-a' OR s LIKE '%never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- A single-character wildcard before the anchor: regexp `^ta.b$`. `.` matches a newline on both
-- paths (re2 is built with `RE_DOT_NL`, Vectorscan with `HS_FLAG_DOTALL`), only `$` differs.
SELECT 'underscore / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta_b' OR s LIKE 'ne_er' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'underscore / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta_b' OR s LIKE 'ne_er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'underscore / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta_b' OR s LIKE 'ne_er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- A `%` wildcard that is not at the end of the pattern: regexp `^ta.*b$`.
SELECT 'inner percent / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta%b' OR s LIKE 'ne%er' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'inner percent / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta%b' OR s LIKE 'ne%er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'inner percent / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta%b' OR s LIKE 'ne%er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- `ILIKE` takes the same path with a `(?i)` prefix: regexp `(?i)^tenant\-a$`.
SELECT 'ilike / rewrite off', count() FROM t_or_like_end_anchor WHERE s ILIKE 'TENANT-A' OR s ILIKE 'NEVER' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'ilike / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s ILIKE 'TENANT-A' OR s ILIKE 'NEVER' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'ilike / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s ILIKE 'TENANT-A' OR s ILIKE 'NEVER' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- The empty pattern is anchored on both ends: regexp `^$`, which a PCRE `$` lets match a lone newline.
SELECT 'empty pattern / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE '' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'empty pattern / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE '' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'empty pattern / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE '' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- A group is kept or rewritten as a whole: one end-anchored branch among prefix branches keeps the
-- whole group on its original branches.
SELECT 'mixed group / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'never%' OR s LIKE 'tenant-a' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'mixed group / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'never%' OR s LIKE 'tenant-a' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'mixed group / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'never%' OR s LIKE 'tenant-a' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- A trailing `%` after a literal `$` emits no anchor (regexp `^a\$`), so such patterns stay eligible
-- for `multiMatchAny`. The literal dollar sign must not be mistaken for the end anchor.
SELECT 'escaped dollar / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'a$%' OR s LIKE 'b$%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'escaped dollar / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'a$%' OR s LIKE 'b$%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'escaped dollar / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'a$%' OR s LIKE 'b$%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- Prefix patterns emit no anchor (regexp `^tenant`) and keep being rewritten into `multiMatchAny`.
SELECT 'prefix / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant%' OR s LIKE 'never%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'prefix / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant%' OR s LIKE 'never%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'prefix / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant%' OR s LIKE 'never%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

DROP TABLE t_or_like_end_anchor;
