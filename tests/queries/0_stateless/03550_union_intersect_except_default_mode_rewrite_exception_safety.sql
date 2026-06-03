(SELECT 1 EXCEPT SELECT 1) SETTINGS except_default_mode = ''; -- { serverError EXPECTED_ALL_OR_DISTINCT }
