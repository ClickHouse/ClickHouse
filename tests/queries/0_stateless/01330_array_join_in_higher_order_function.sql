SELECT arrayMap(x -> arrayJoin([x, 1]), [1, 2]); -- { serverError BAD_ARGUMENTS }
