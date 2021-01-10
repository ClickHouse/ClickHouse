SELECT normalizeQueryWithNames('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`'),
    normalizedQueryHashWithNames('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`')
  = normalizedQueryHashWithNames('SELECT 2 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef`');
