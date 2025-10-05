-- Test for issue #87840: mortonEncode with empty tuple should fail gracefully
SELECT mortonEncode(()); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }

-- hilbertEncode should also reject empty tuple (uses same base class)
SELECT hilbertEncode(()); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
