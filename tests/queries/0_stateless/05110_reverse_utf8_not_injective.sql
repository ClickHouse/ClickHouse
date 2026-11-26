-- `reverseUTF8` is not injective: a byte announcing a multi-byte sequence with fewer bytes remaining
-- than it needs is reversed as a single byte, so two different inputs give the same output. It used to
-- declare itself injective, and `optimize_injective_functions_in_group_by` and
-- `optimize_injective_functions_inside_uniq` (both on by default) then grouped and counted by the
-- argument, returning one group per argument instead of one per result value.

SELECT 'the two inputs that collide';
SELECT hex(reverseUTF8('a\xC2')), hex(reverseUTF8('\xC2a')), reverseUTF8('a\xC2') = reverseUTF8('\xC2a');

SELECT 'GROUP BY the result';
SELECT count() FROM (SELECT reverseUTF8(s) AS k FROM (SELECT arrayJoin(['a\xC2', '\xC2a']) AS s) GROUP BY k);
SELECT count() FROM (SELECT reverseUTF8(s) AS k FROM (SELECT arrayJoin(['a\xC2', '\xC2a']) AS s) GROUP BY k)
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT 'uniqExact of the result';
SELECT uniqExact(reverseUTF8(s)) FROM (SELECT arrayJoin(['a\xC2', '\xC2a']) AS s);
SELECT uniqExact(reverseUTF8(s)) FROM (SELECT arrayJoin(['a\xC2', '\xC2a']) AS s)
SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT 'valid UTF-8 still groups by the result';
SELECT count(), min(k) FROM (SELECT reverseUTF8(s) AS k FROM (SELECT arrayJoin(['abc', 'cba', 'abc']) AS s) GROUP BY k);
