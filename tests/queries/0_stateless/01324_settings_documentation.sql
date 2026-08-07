SELECT 'Settings description should start with capital letter';
SELECT name, description FROM system.settings WHERE substring(description, 1, 1) != upper(substring(description, 1, 1));
