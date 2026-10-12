SELECT dummy AND count() + NULL AS h FROM system.one GROUP BY dummy HAVING h UNION ALL SELECT 1;
