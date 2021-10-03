SELECT 1, * FROM (SELECT NULL AS `1`); -- { serverError 352 }
SELECT '7', 'xyz', * FROM (SELECT NULL AS `'xyz'`); -- { serverError 352 }
