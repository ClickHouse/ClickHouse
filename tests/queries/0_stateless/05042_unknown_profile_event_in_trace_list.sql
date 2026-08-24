-- A typo in `trace_profile_events_list` should give a readable error, not `std::out_of_range`.
SELECT 1 SETTINGS trace_profile_events = 1, trace_profile_events_list = 'DiskS3PutObject,DiskS3CommitBlockList'; -- { serverError BAD_ARGUMENTS }

-- Spaces around the names and a trailing comma are allowed.
SELECT 2 SETTINGS trace_profile_events = 1, trace_profile_events_list = ' Query, SelectQuery, ';
