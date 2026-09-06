-- A pure inline flag group such as `(?i)` is zero-width for RE2: a quantifier written after it
-- applies to the atom before the group, so `^ab(?i)*c` requires only `a`. The fixed-prefix extractor
-- used for primary-key pruning of `match` reported `ab`, and granules holding only matching rows
-- outside `['ab', 'ac')` were pruned - rows silently disappeared from the result.

DROP TABLE IF EXISTS t_prefix_flag_group;
CREATE TABLE t_prefix_flag_group (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_prefix_flag_group VALUES ('abc'), ('ac'), ('zz');

SELECT 'a quantifier behind a flag group';
SELECT match('ac', '^ab(?i)*c'), match('abc', '^ab(?i)*c');
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab(?i)*c');
SELECT countIf(match(s, '^ab(?i)*c')) FROM t_prefix_flag_group;

SELECT 'the other quantifiers that allow zero occurrences';
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab(?i)?c');
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab(?i){0,2}c');
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab(?ims)*c');

SELECT 'an empty \Q\E quote in the same place';
SELECT match('ac', '^ab\Q\E*c');
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab\Q\E*c');
SELECT countIf(match(s, '^ab\Q\E*c')) FROM t_prefix_flag_group;

SELECT 'a flag group without a quantifier still gives the whole prefix';
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab(?i)c');
SELECT countIf(match(s, '^ab(?i)c')) FROM t_prefix_flag_group;

SELECT 'and a group with a body keeps the prefix, because the quantifier applies to the group';
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab(c)*');
SELECT countIf(match(s, '^ab(c)*')) FROM t_prefix_flag_group;
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab(?:c)*');
SELECT countIf(match(s, '^ab(?:c)*')) FROM t_prefix_flag_group;

SELECT 'a plain quantifier over a literal is unchanged';
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^ab*c');
SELECT countIf(match(s, '^ab*c')) FROM t_prefix_flag_group;
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^abc');
SELECT count() FROM t_prefix_flag_group WHERE match(s, '^zz');

DROP TABLE t_prefix_flag_group;
