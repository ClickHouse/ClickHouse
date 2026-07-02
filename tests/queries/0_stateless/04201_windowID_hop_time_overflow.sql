-- Test: exercises `windowID` time-overflow guard in `TimeWindowImpl<WINDOW_ID>::executeHopSlice`.
-- Covers: src/Functions/FunctionsTimeWindow.cpp:580 — `if (wstart > wend)` overflow check
--   inside `executeHopSlice` (the `windowID` hop-slice path).
-- The PR adds the same overflow guard in two distinct paths: `executeHop` (for `hop`) and
-- `executeHopSlice` (for `windowID`). The PR's own test only triggers the former; this test
-- covers the latter, where `wend = AddTime<kind>(wstart, hop_num_units)` wraps around UInt32.

SET allow_experimental_window_view = 1;

-- 3-arg form: dispatchForColumns -> dispatchForHopColumns -> executeHopSlice<UInt32, Hour>
-- Time near max DateTime (2106-02-07 06:28:14) plus 1-hour hop overflows wstart+hop_num_units.
SELECT windowID(toDateTime('2106-02-07 06:00:00'), INTERVAL 1 HOUR, INTERVAL 1 HOUR); -- { serverError BAD_ARGUMENTS }

-- 4-arg form (same code path, with explicit timezone).
SELECT windowID(toDateTime('2106-02-07 00:00:00'), INTERVAL 1 DAY, INTERVAL 1 DAY, 'UTC'); -- { serverError BAD_ARGUMENTS }

-- Sanity: a non-overflowing input on the same path returns a value (regression guard for
-- false positives if the overflow check were inverted).
SELECT windowID(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 HOUR, INTERVAL 2 HOUR, 'UTC') > 0;
