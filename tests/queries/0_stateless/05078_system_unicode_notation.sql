-- Coverage for src/Storages/System/StorageSystemUnicode.cpp.
-- The `notation` column (U+XXXX representation) is never queried in CI.
-- Exercises the 4-hex (U+XXXX), 5-hex (U+XXXXX), and 6-hex (U+XXXXXX) notation
-- paths as well as the false branch of the code_point column mask check.

-- 4-digit notation path (code_point_value <= 0xFFFF): U+0041 = 'A'
SELECT notation FROM system.unicode WHERE code_point = 'A';

-- 5-digit notation path (0x10000 <= code_point_value <= 0xFFFFF): U+1F602 = '😂'
SELECT notation FROM system.unicode WHERE code_point = '😂';

-- 6-digit notation path (code_point_value > 0xFFFFF): U+100000 (Supplementary Private Use Area-B)
SELECT notation FROM system.unicode WHERE code_point_value = 1048576;
