-- input_format_force_null_for_omitted_fields rejects an omitted non-Nullable column in the
-- JSONColumns family, and accepts an omitted Nullable one.
SELECT * FROM format(JSONColumns, 'foo UInt32, bar UInt32', '{"foo":[1,2,3]}')
SETTINGS input_format_force_null_for_omitted_fields = 1; -- { serverError TYPE_MISMATCH }

SELECT * FROM (
    SELECT * FROM format(JSONColumns, 'foo UInt32, bar Nullable(UInt32)', '{"foo":[1,2,3]}')
    SETTINGS input_format_force_null_for_omitted_fields = 1
) ORDER BY foo;
