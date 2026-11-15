-- Test for issue #87840: mortonEncode with empty tuple should fail gracefully
SELECT mortonEncode(()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- hilbertEncode should also reject empty tuple (uses same base class)
SELECT hilbertEncode(()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
