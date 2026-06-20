-- Regression test for the anchored capture-then-truncate fast path of
-- replaceRegexpAll / replaceRegexpOne (REGEXP_REPLACE), e.g. ClickBench Q28's
-- `^https?://(?:www\.)?([^/]+)/.*$` with replacement `\1`: the fast path strips the
-- trailing `.*$` and emits the captured group, falling back to the full regexp on rows
-- whose discarded suffix contains a newline. Covers firing, the newline fallback and
-- shapes that must NOT trigger the optimization (still correct via the default path).

DROP TABLE IF EXISTS t_anchored_extract;
CREATE TABLE t_anchored_extract (id UInt32, s String) ENGINE = Memory;
INSERT INTO t_anchored_extract VALUES
    (1, 'http://www.example.com/path/to/page?q=1'),
    (2, 'https://example.org/a'),
    (3, 'http://example.com'),
    (4, 'http://www.x.com/'),
    (5, 'http://a.com/b/c/d'),
    (6, 'http://x.com:8080/a'),
    (7, 'https://user:pass@h.com/p'),
    (8, 'http://日本.example/パス/q'),
    (9, 'HTTP://UPPER.COM/x'),
    (10, 'http://only.host.no.slash'),
    (11, 'http://trailing.slash.com/'),
    (12, 'https://www.sub.dom.co.uk/a/b?x=y#z'),
    (13, 'http://1.2.3.4/page'),
    (14, 'http://[::1]/v6'),
    (15, 'http://x/y'),
    (16, 'https://x.com'),
    (17, 'http://a b.com/p'),
    (18, 'ftp://nomatch.com/a'),
    (19, 'hello world'),
    (20, ''),
    (21, 'http://www.www.com/x'),
    (22, 'http://h.com/very/long/path/segment/here?a=1&b=2'),
    (23, 'http://www.x.com/a\nb'),
    (24, 'http://x.com/\nfoo'),
    (25, 'pre\nhttp://x.com/a'),
    (26, 'http://h.com/a\n'),
    (27, 'http://h.com/\n'),
    (28, '\nhttp://h.com/a'),
    (29, 'http://h.com/a\r\nb'),
    (30, 'http://h.com/a\tb'),
    (31, 'http://h.com/a\0b'),
    (32, 'a\nb'),
    (33, 'abc/def'),
    (34, 'abc'),
    (35, 'a/b/c'),
    (36, 'x/'),
    (37, 'noslashhere'),
    (38, 'a/'),
    (39, '/leading'),
    (40, 'a//b'),
    (41, '123-abc'),
    (42, 'abc-1'),
    (43, 'a.b.c'),
    (44, 'ab.c');

-- Part A: independent oracle. For anchored single-match bare-backref patterns the result
-- must equal if(match(s,P), extractGroups(s,P)[N], s). match()/extractGroups() do NOT use
-- the optimized code path, so this independently validates it. Restricted to newline-free
-- inputs because match()/extractGroups() treat '.' as matching '\n' (unlike replaceRegexp).
-- Each line prints 0 mismatches.
SELECT 'q28_host' AS pattern, sum(replaceRegexpAll(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') != if(match(s, '^https?://(?:www\\.)?([^/]+)/.*$'), extractGroups(s, '^https?://(?:www\\.)?([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'simple_first_seg' AS pattern, sum(replaceRegexpAll(s, '^([^/]+)/.*$', '\\1') != if(match(s, '^([^/]+)/.*$'), extractGroups(s, '^([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'scheme_group1' AS pattern, sum(replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\1') != if(match(s, '^(https?)://([^/]+)/.*$'), extractGroups(s, '^(https?)://([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'host_group2' AS pattern, sum(replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\2') != if(match(s, '^(https?)://([^/]+)/.*$'), extractGroups(s, '^(https?)://([^/]+)/.*$')[2], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'escaped_dot_prefix' AS pattern, sum(replaceRegexpAll(s, '^(a)\\..*$', '\\1') != if(match(s, '^(a)\\..*$'), extractGroups(s, '^(a)\\..*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'explicit_nonnl_tail' AS pattern, sum(replaceRegexpAll(s, '^([^/]+)/[^\\n]*$', '\\1') != if(match(s, '^([^/]+)/[^\\n]*$'), extractGroups(s, '^([^/]+)/[^\\n]*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'backslash_A_z' AS pattern, sum(replaceRegexpAll(s, '\\A([^/]+)/.*\\z', '\\1') != if(match(s, '\\A([^/]+)/.*\\z'), extractGroups(s, '\\A([^/]+)/.*\\z')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'dotall_host' AS pattern, sum(replaceRegexpAll(s, '(?s)^([^/]+)/.*$', '\\1') != if(match(s, '(?s)^([^/]+)/.*$'), extractGroups(s, '(?s)^([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'anchored_no_dotstar' AS pattern, sum(replaceRegexpAll(s, '^(a)bc$', '\\1') != if(match(s, '^(a)bc$'), extractGroups(s, '^(a)bc$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'astar_tail' AS pattern, sum(replaceRegexpAll(s, '^https?://([^/]+)/a*$', '\\1') != if(match(s, '^https?://([^/]+)/a*$'), extractGroups(s, '^https?://([^/]+)/a*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'greedy_last_slash' AS pattern, sum(replaceRegexpAll(s, '^(.*)/.*$', '\\1') != if(match(s, '^(.*)/.*$'), extractGroups(s, '^(.*)/.*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'lazy_first_slash' AS pattern, sum(replaceRegexpAll(s, '^(.*?)/.*$', '\\1') != if(match(s, '^(.*?)/.*$'), extractGroups(s, '^(.*?)/.*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;
SELECT 'digits_prefix' AS pattern, sum(replaceRegexpAll(s, '^([0-9]+)-.*$', '\\1') != if(match(s, '^([0-9]+)-.*$'), extractGroups(s, '^([0-9]+)-.*$')[1], s)) AS mismatches FROM t_anchored_extract WHERE position(s, '\n') = 0;

-- Part B: concrete golden behaviour (includes newline fallback rows and non-firing shapes).
-- Output columns: label, id, s, replaceRegexpAll, replaceRegexpOne.
SELECT 'FIRE q28 \\1' AS t, id, s, replaceRegexpAll(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS all_, replaceRegexpOne(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE scheme \\1' AS t, id, s, replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\1') AS all_, replaceRegexpOne(s, '^(https?)://([^/]+)/.*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE host \\2' AS t, id, s, replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\2') AS all_, replaceRegexpOne(s, '^(https?)://([^/]+)/.*$', '\\2') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE dotall \\1' AS t, id, s, replaceRegexpAll(s, '(?s)^([^/]+)/.*$', '\\1') AS all_, replaceRegexpOne(s, '(?s)^([^/]+)/.*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE explicit[^nl] \\1' AS t, id, s, replaceRegexpAll(s, '^([^/]+)/[^\\n]*$', '\\1') AS all_, replaceRegexpOne(s, '^([^/]+)/[^\\n]*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE repl x\\1' AS t, id, s, replaceRegexpAll(s, '^https?://(?:www\\.)?([^/]+)/.*$', 'x\\1') AS all_, replaceRegexpOne(s, '^https?://(?:www\\.)?([^/]+)/.*$', 'x\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE repl \\2-\\1' AS t, id, s, replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\2-\\1') AS all_, replaceRegexpOne(s, '^(https?)://([^/]+)/.*$', '\\2-\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE astar_tail \\1' AS t, id, s, replaceRegexpAll(s, '^https?://([^/]+)/a*$', '\\1') AS all_, replaceRegexpOne(s, '^https?://([^/]+)/a*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE no_dotstar \\1' AS t, id, s, replaceRegexpAll(s, '^(a)bc$', '\\1') AS all_, replaceRegexpOne(s, '^(a)bc$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE no_begin \\1' AS t, id, s, replaceRegexpAll(s, '(foo).*$', '\\1') AS all_, replaceRegexpOne(s, '(foo).*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE no_end \\1' AS t, id, s, replaceRegexpAll(s, '^(foo).*', '\\1') AS all_, replaceRegexpOne(s, '^(foo).*', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE multiline \\1' AS t, id, s, replaceRegexpAll(s, '(?m)^(\\w+)$', '\\1') AS all_, replaceRegexpOne(s, '(?m)^(\\w+)$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE group_is_dotstar' AS t, id, s, replaceRegexpAll(s, '^(.*)$', '\\1') AS all_, replaceRegexpOne(s, '^(.*)$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;

DROP TABLE t_anchored_extract;
