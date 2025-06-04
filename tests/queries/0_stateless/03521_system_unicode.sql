
SELECT code_point, code_point_value FROM system.unicode WHERE code_point = '😂';

SELECT code_point, code_point_value FROM system.unicode WHERE emoji_presentation = 1 ORDER BY code_point_value LIMIT 5;

SELECT lowercase_mapping FROM system.unicode WHERE code_point = 'A';