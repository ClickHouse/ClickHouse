-- Tags: no-parallel
SYSTEM DROP REPLICA 'r1' FROM ZKPATH ''; -- { serverError BAD_ARGUMENTS }
