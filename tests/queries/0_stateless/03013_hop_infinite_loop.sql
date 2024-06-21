SET allow_experimental_window_view = 1;
SELECT hop(toDateTime(0), INTERVAL 1 DAY, INTERVAL 3 DAY); -- { serverError BAD_ARGUMENTS }
