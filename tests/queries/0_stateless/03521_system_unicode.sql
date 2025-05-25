
SELECT code_point, code_point_value FROM system.unicode WHERE code_point = '😂';

SELECT code_point, code_point_value FROM system.unicode WHERE Emoji_Presentation = 1 ORDER BY code_point_value LIMIT 5;

SELECT Lowercase_Mapping FROM system.unicode WHERE code_point = 'A';