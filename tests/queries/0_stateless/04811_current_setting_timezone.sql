-- `current_setting('timezone')` must report the effective timezone of the query,
-- following the `session_timezone` setting, not a hardcoded constant.
SELECT current_setting('timezone') SETTINGS session_timezone = 'Asia/Tokyo';
SELECT current_setting('timezone') SETTINGS session_timezone = 'UTC';
-- Without an override it agrees with the `timezone` function.
SELECT current_setting('timezone') = timezone();
