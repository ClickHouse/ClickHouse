SELECT runningAccumulate(string_state)
FROM (
  SELECT  argMaxState(repeat('a', 48), 1) AS string_state
)