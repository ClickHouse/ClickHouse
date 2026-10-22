-- Test: exercises `fieldToNumber<bool>` with out-of-range Float64 values
-- Covers: src/Core/SettingsFields.cpp:129-130 — bool special case for out-of-range float
-- The bool special case uses naive comparison (not `accurate::lessOp`) because
-- `make_unsigned_t<bool>` is ill-formed. This test verifies the check works correctly.

-- Out-of-range positive float should throw
SET optimize_move_to_prewhere = 1.5; -- { serverError CANNOT_CONVERT_TYPE }
SET optimize_move_to_prewhere = 2.0; -- { serverError CANNOT_CONVERT_TYPE }

-- Out-of-range negative float should throw
SET optimize_move_to_prewhere = -0.5; -- { serverError CANNOT_CONVERT_TYPE }
SET optimize_move_to_prewhere = -1.0; -- { serverError CANNOT_CONVERT_TYPE }

-- In-range floats should work (truncate to bool: 0->false, non-zero->true)
SET optimize_move_to_prewhere = 1.0;
SELECT getSetting('optimize_move_to_prewhere') AS v1 FORMAT TabSeparated;

SET optimize_move_to_prewhere = 0.0;
SELECT getSetting('optimize_move_to_prewhere') AS v2 FORMAT TabSeparated;

-- Edge case: values between 0 and 1 should work (truncate to true since non-zero)
SET optimize_move_to_prewhere = 0.5;
SELECT getSetting('optimize_move_to_prewhere') AS v3 FORMAT TabSeparated;

SET optimize_move_to_prewhere = 0.999;
SELECT getSetting('optimize_move_to_prewhere') AS v4 FORMAT TabSeparated;
