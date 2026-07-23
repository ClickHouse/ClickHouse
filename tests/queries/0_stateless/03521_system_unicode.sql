-- Tags: no-fasttest
SELECT code_point, code_point_value FROM system.unicode WHERE code_point = '😂';

SELECT code_point, code_point_value FROM system.unicode WHERE emoji_presentation = 1 ORDER BY code_point_value LIMIT 5;

SELECT code_point, lowercase_mapping FROM system.unicode WHERE code_point = 'A' or code_point = 'Ä' or code_point = 'Ω' order by code_point;
-- special mapping
SELECT code_point, uppercase_mapping, simple_uppercase_mapping FROM system.unicode WHERE code_point = 'ß';
-- no language-specific mappings 
SELECT code_point, uppercase_mapping, simple_uppercase_mapping FROM system.unicode WHERE code_point = 'i';

SELECT code_point, script_extensions, identifier_type FROM system.unicode WHERE code_point = 'A';
