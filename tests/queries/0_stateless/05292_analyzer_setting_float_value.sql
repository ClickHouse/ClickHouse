SET enable_analyzer = 1.0;
SET allow_experimental_analyzer = 0.0;
SELECT toUInt8(getSetting('enable_analyzer'));
SELECT 1 SETTINGS enable_analyzer = 0.0;
