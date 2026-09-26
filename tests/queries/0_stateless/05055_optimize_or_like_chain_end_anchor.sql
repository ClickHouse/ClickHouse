-- `optimize_or_like_chain` rewrites an `OR` chain of `LIKE`/`ILIKE` into `multiMatchAny`, which runs
-- through Vectorscan, where `$` also matches before a final newline (PCRE); the original `like` uses
-- re2, where `$` is the absolute end. `likePatternToRegexp` anchors every pattern not ending in `%`,
-- so the rewrite used to widen such filters: `s LIKE 'tenant-a'` also returned `'tenant-a\n'`.
-- Results must not depend on `optimize_or_like_chain` or `allow_hyperscan`.

SET optimize_or_like_chain_min_patterns = 1;

DROP TABLE IF EXISTS t_or_like_end_anchor;
CREATE TABLE t_or_like_end_anchor (s String) ENGINE = Memory;

-- The values ending in a newline are excluded by the patterns below, but accepted by a PCRE `$`.
INSERT INTO t_or_like_end_anchor VALUES ('tenant-a'), ('tenant-a\n'), ('tenant-a\n\n'), ('x-tenant-a'), ('x-tenant-a\n'), ('taxb'), ('ta\nb'), ('ta\nb\n'), (''), ('\n'), ('a$'), ('a$\n');

-- regexp `^tenant\-a$`
SELECT 'exact / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant-a' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'exact / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant-a' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'exact / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant-a' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- regexp `tenant\-a$`
SELECT 'suffix / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE '%tenant-a' OR s LIKE '%never' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'suffix / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE '%tenant-a' OR s LIKE '%never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'suffix / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE '%tenant-a' OR s LIKE '%never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- regexp `^ta.b$`. `.` matches a newline on both paths (`RE_DOT_NL`, `HS_FLAG_DOTALL`), only `$` differs.
SELECT 'underscore / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta_b' OR s LIKE 'ne_er' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'underscore / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta_b' OR s LIKE 'ne_er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'underscore / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta_b' OR s LIKE 'ne_er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- regexp `^ta.*b$`
SELECT 'inner percent / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta%b' OR s LIKE 'ne%er' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'inner percent / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta%b' OR s LIKE 'ne%er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'inner percent / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'ta%b' OR s LIKE 'ne%er' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- regexp `(?i)^tenant\-a$`
SELECT 'ilike / rewrite off', count() FROM t_or_like_end_anchor WHERE s ILIKE 'TENANT-A' OR s ILIKE 'NEVER' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'ilike / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s ILIKE 'TENANT-A' OR s ILIKE 'NEVER' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'ilike / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s ILIKE 'TENANT-A' OR s ILIKE 'NEVER' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- regexp `^$`, which a PCRE `$` lets match a lone newline
SELECT 'empty pattern / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE '' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'empty pattern / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE '' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'empty pattern / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE '' OR s LIKE 'never' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- A group is kept or rewritten as a whole: one anchored branch keeps the whole group unrewritten.
SELECT 'mixed group / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'never%' OR s LIKE 'tenant-a' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'mixed group / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'never%' OR s LIKE 'tenant-a' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'mixed group / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'never%' OR s LIKE 'tenant-a' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- A trailing `%` after a literal `$` emits no anchor (regexp `^a\$`): still eligible, still correct.
SELECT 'escaped dollar / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'a$%' OR s LIKE 'b$%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'escaped dollar / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'a$%' OR s LIKE 'b$%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'escaped dollar / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'a$%' OR s LIKE 'b$%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

-- Prefix patterns emit no anchor (regexp `^tenant`) and keep being rewritten.
SELECT 'prefix / rewrite off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant%' OR s LIKE 'never%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
SELECT 'prefix / rewrite on, hyperscan on', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant%' OR s LIKE 'never%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT 'prefix / rewrite on, hyperscan off', count() FROM t_or_like_end_anchor WHERE s LIKE 'tenant%' OR s LIKE 'never%' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;

DROP TABLE t_or_like_end_anchor;
