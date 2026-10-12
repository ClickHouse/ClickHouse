-- At most 127 capturing groups, as in `extractAllGroupsHorizontal` and `extractAllGroupsVertical`.
SELECT length(extractGroups(repeat('a', 127), repeat('(\\w)', 127)));
SELECT extractGroups(repeat('a', 128), repeat('(\\w)', 128)); -- { serverError BAD_ARGUMENTS }
