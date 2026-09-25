-- Tags: no-fasttest
SELECT code_point, code_point_value FROM system.unicode WHERE code_point = '😂';

SELECT code_point, code_point_value FROM system.unicode WHERE emoji_presentation = 1 ORDER BY code_point_value LIMIT 5;

SELECT code_point, lowercase_mapping FROM system.unicode WHERE code_point = 'A' or code_point = 'Ä' or code_point = 'Ω' order by code_point;
-- special mapping
SELECT code_point, uppercase_mapping, simple_uppercase_mapping FROM system.unicode WHERE code_point = 'ß';
-- no language-specific mappings 
SELECT code_point, uppercase_mapping, simple_uppercase_mapping FROM system.unicode WHERE code_point = 'i';

SELECT code_point, script_extensions, identifier_type FROM system.unicode WHERE code_point = 'A';

-- Coverage for StorageSystemUnicode.cpp: the `notation` column (U+XXXX representation)
-- was never queried in CI. Exercises 4-hex (<=0xFFFF), 5-hex (0x10000-0xFFFFF), 6-hex (>0xFFFFF).
-- 4-digit notation path (code_point_value <= 0xFFFF): U+0041 = 'A'
SELECT notation FROM system.unicode WHERE code_point = 'A';

-- 5-digit notation path (0x10000 <= code_point_value <= 0xFFFFF): U+1F602 = '😂'
SELECT notation FROM system.unicode WHERE code_point = '😂';

-- 6-digit notation path (code_point_value > 0xFFFFF): U+100000 (Supplementary Private Use Area-B)
SELECT notation FROM system.unicode WHERE code_point_value = 1048576;
