-- Tags: no-fasttest
-- ^ depends on ICU library
-- https://github.com/ClickHouse/ClickHouse/issues/120604
SELECT code_point_value, length(notation) FROM system.unicode WHERE code_point_value IN (65, 128514, 1048576) ORDER BY code_point_value;
SELECT count() FROM system.unicode WHERE notation IN ('U+0041', 'U+1F602', 'U+100000');
SELECT countIf(position(notation, '\0') > 0) FROM system.unicode;
