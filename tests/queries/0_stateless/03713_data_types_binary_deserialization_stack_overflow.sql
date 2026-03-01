select * from format(RowBinaryWithNamesAndTypes, x'010178' || repeat(x'1e', 1000000)) settings input_format_binary_decode_types_in_binary_format=1; -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
