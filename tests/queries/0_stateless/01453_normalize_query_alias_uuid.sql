SELECT normalizeQuery('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`'),
    normalizedQueryHash('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`')
  = normalizedQueryHash('SELECT 2 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef`');
