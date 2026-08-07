SET enable_analyzer = DEFAULT;
SELECT name, value, changed from system.settings where name IN ('allow_experimental_analyzer', 'enable_analyzer') ORDER BY name;
SET compatibility = '24.8';
SELECT name, value, changed from system.settings where name IN ('allow_experimental_analyzer', 'enable_analyzer') ORDER BY name;
SET compatibility = '24.3';
SELECT name, value, changed from system.settings where name IN ('allow_experimental_analyzer', 'enable_analyzer') ORDER BY name;
SET compatibility = '24.1';
SELECT name, value, changed from system.settings where name IN ('allow_experimental_analyzer', 'enable_analyzer') ORDER BY name;
