SELECT normalizeQueryKeepNames('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`'),
    normalizedQueryHashKeepNames('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`')
  = normalizedQueryHashKeepNames('SELECT 2 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef`');
