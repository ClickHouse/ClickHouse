-- A very negative value of a Seconds setting is rejected.
SELECT 1 SETTINGS max_execution_time = -1e18; -- { clientError BAD_ARGUMENTS }
