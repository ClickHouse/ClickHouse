-- A matched capture group is a whole field, so the value must consume it entirely.

SELECT '-- Escaped: a partially parsed value is rejected';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D2D310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v UInt64', unhex('763D2D310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D316162630A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D312E390A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(Int64)', unhex('763D313278797A0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(Float64)', unhex('763D312E356162630A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(Date)', unhex('763D323032302D30312D30316A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Array(UInt64)', unhex('763D5B312C325D78797A0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D6E756C6C0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }

SELECT '-- Escaped: a tail after the null marker is rejected';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D5C4E6A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v UInt64', unhex('763D5C4E6A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped', input_format_null_as_default = 1; -- { serverError INCORRECT_DATA }

SELECT '-- CSV: a partially parsed value is rejected';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D316162630A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D312E390A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D22312278797A0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV'; -- { serverError INCORRECT_DATA }

SELECT '-- JSON: a partially parsed value is rejected';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D316162630A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D312E390A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(Bool)', unhex('763D316162630A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(Date)', unhex('763D323032302D30312D30316A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON'; -- { serverError INCORRECT_DATA }

SELECT '-- CSV and JSON accept trailing whitespace, like the formats they mirror';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31200A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31200A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31090A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31090A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D310D0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31200A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV', input_format_csv_allow_whitespace_or_tab_as_delimiter = 1; -- { serverError INCORRECT_DATA }

SELECT '-- whitespace is not an amnesty for what follows it';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31206162630A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31206162630A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON'; -- { serverError INCORRECT_DATA }

SELECT '-- the other rules reject trailing whitespace, like the formats they mirror';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31200A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31200A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Quoted'; -- { serverError INCORRECT_DATA }
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31200A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Raw'; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }

SELECT '-- Raw reads a capture containing a tab as a whole, at the default rule and explicitly';
SELECT hex(v) FROM format(Regexp, 'v String', unhex('763D616263096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT hex(v) FROM format(Regexp, 'v String', unhex('763D616263096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Raw';
SELECT hex(v) FROM format(Regexp, 'v LowCardinality(String)', unhex('763D616263096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT hex(v) FROM format(Regexp, 'v Nullable(String)', unhex('763D616263096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT hex(v) FROM format(Regexp, 'v LowCardinality(Nullable(String))', unhex('763D616263096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$';

SELECT '-- Raw: a tab that cannot belong to the value is rejected';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D31096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$'; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
SELECT * FROM format(Regexp, 'v Nullable(Float64)', unhex('763D31096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$'; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
SELECT * FROM format(Regexp, 'v UInt64', unhex('763D31096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$', input_format_null_as_default = 1; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
SELECT * FROM format(Regexp, 'v UInt64', unhex('763D5C4E096A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$', input_format_null_as_default = 1; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }

SELECT '-- Raw: captures without a tab are unchanged';
SELECT hex(v) FROM format(Regexp, 'v String', unhex('763D616263206A756E6B0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT hex(v) FROM format(Regexp, 'v String', unhex('763D48656C6C6F2C20776F726C64210A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT hex(v) FROM format(Regexp, 'v LowCardinality(String)', unhex('763D6162630A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Raw';

SELECT '-- Raw null tokens';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v Nullable(String)', unhex('763D5C4E0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v Nullable(String)', unhex('763D4E554C4C0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v Nullable(String)', unhex('763D58595A0A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'XYZ';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v Nullable(String)', unhex('763D5C4E0A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'XYZ';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v LowCardinality(Nullable(String))', unhex('763D5C4E0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v LowCardinality(Nullable(String))', unhex('763D4E554C4C0A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v LowCardinality(Nullable(String))', unhex('763D58595A0A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'XYZ';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v LowCardinality(Nullable(String))', unhex('763D5C4E0A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'XYZ';

SELECT '-- Raw: a tab-containing null token is a value where null handling is off';
SELECT hex(v) FROM format(Regexp, 'v String', unhex('763D6109620A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'a\tb', input_format_null_as_default = 0;
SELECT isNull(v), hex(v) FROM format(Regexp, 'v Nullable(String)', unhex('763D6109620A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'a\tb', input_format_null_as_default = 0;
SELECT hex(v) FROM format(Regexp, 'v String', unhex('763D6109620A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'a\tb';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v LowCardinality(Nullable(String))', unhex('763D6109620A')) SETTINGS format_regexp = '^v=(.+)$', format_tsv_null_representation = 'a\tb', input_format_null_as_default = 0;

SELECT '-- Raw: a null token followed by a tab is a value, not a null';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v Nullable(String)', unhex('763D5C4E09780A')) SETTINGS format_regexp = '^v=(.+)$';
SELECT isNull(v), hex(v) FROM format(Regexp, 'v LowCardinality(Nullable(String))', unhex('763D5C4E09780A')) SETTINGS format_regexp = '^v=(.+)$';

SELECT '-- well-formed fields are still accepted';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, 'v String', unhex('763D737472310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, 'v Array(UInt64)', unhex('763D5B312C322C335D0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, 'v Nullable(Date)', unhex('763D323032302D30312D30310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT isNull(v) FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D5C4E0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT hex(v) FROM format(Regexp, 'v String', unhex('763D615C74620A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, 'v String', unhex('763D2273747231220A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'CSV';
SELECT * FROM format(Regexp, 'v String', unhex('763D2273747231220A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON';
SELECT * FROM format(Regexp, 'v Array(UInt64)', unhex('763D5B312C322C335D0A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'JSON';
SELECT * FROM format(Regexp, 'v Nullable(UInt64)', unhex('763D310A')) SETTINGS format_regexp = '^v=(.+)$', format_regexp_escaping_rule = 'Quoted';
