-- Tags: no-fasttest
-- ^ depends on ICU library

SELECT code_point, code_point_value, name, block FROM system.unicode WHERE numeric_type = 'Digit' AND block = 'Ethiopic' ORDER BY code_point;
SELECT code_point, code_point_value, name, block FROM system.unicode WHERE block = 'Emoticons' AND name LIKE '%CRY%' ORDER BY code_point;
