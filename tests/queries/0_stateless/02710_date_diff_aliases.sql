SELECT name FROM system.functions
WHERE name = 'date_diff'
   OR name = 'DATE_DIFF'
   OR name = 'timestampDiff'
   OR name = 'timestamp_diff'
   OR name = 'TIMESTAMP_DIFF'
ORDER BY name;
