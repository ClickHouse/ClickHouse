SET allow_deprecated_error_prone_window_functions = 1;

SELECT runningAccumulate(string_state)
FROM (
  SELECT  argMaxState(repeat('a', 48), 1) AS string_state
)
