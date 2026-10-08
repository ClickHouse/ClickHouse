select '{"skip_abc" : 42}'::JSON(SKIP REGEXP 'skip') settings type_json_use_partial_match_to_skip_paths_by_regexp=0;
select '{"skip_abc" : 42}'::JSON(SKIP REGEXP 'skip') settings type_json_use_partial_match_to_skip_paths_by_regexp=1;
select '{"skip_abc" : 42}'::JSON(SKIP REGEXP 'skip.*') settings type_json_use_partial_match_to_skip_paths_by_regexp=0;
select '{"skip_abc" : 42}'::JSON(SKIP REGEXP 'skip.*') settings type_json_use_partial_match_to_skip_paths_by_regexp=1;
