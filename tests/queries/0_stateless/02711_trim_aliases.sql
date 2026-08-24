SELECT name FROM system.functions
WHERE name = 'ltrim'
   OR name = 'rtrim'
   OR name = 'trim'
ORDER BY name;
