-- The timezone is pinned on every argument: an unqualified DateTime literal is interpreted in
-- the session timezone, which moves the value across the overflow boundary.
-- A hop window ending past the DateTime range must be rejected, not wrapped around.
SELECT hop(toDateTime('2106-02-07 06:00:00', 'UTC'), INTERVAL 1 HOUR, INTERVAL 1 HOUR, 'UTC'); -- { serverError BAD_ARGUMENTS }

SELECT hop(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 HOUR, INTERVAL 2 HOUR, 'UTC');
