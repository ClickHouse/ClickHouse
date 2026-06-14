-- Test FixedString haystack column with a constant matches-everything needle
-- (vectorFixedConstant shortcut): LIKE '%' / '%%' / any run of '%', and match(., '.*' / '.*?').
SELECT s LIKE '%', s LIKE '%%', s LIKE '%%%', s LIKE '%%%%%%' FROM (SELECT toFixedString(x, 2) AS s FROM (SELECT arrayJoin(['aa', 'bb']) AS x)) ORDER BY s;
SELECT s NOT LIKE '%', s NOT LIKE '%%%' FROM (SELECT toFixedString(x, 2) AS s FROM (SELECT arrayJoin(['aa', 'bb']) AS x)) ORDER BY s;
SELECT s ILIKE '%', s ILIKE '%%%' FROM (SELECT toFixedString(x, 2) AS s FROM (SELECT arrayJoin(['aa', 'bb']) AS x)) ORDER BY s;
SELECT match(s, '.*'), match(s, '.*?') FROM (SELECT toFixedString(x, 2) AS s FROM (SELECT arrayJoin(['aa', 'bb']) AS x)) ORDER BY s;
