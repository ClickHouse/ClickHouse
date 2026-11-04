-- Test Bool serialization via the Raw text format and CRLF TSV input.
-- SerializationBool::serializeTextRaw (src/DataTypes/Serializations/SerializationBool.cpp:364-366)
-- is called by FORMAT TabSeparatedRaw (raw output without TSV escaping) but no existing stateless
-- test selects a Bool column in that format. serializeTextRaw writes "true"/"false" directly;
-- without it the Bool column would fall back to its parent's raw serialization which writes the
-- underlying UInt8 value as a number instead of the boolean keyword.
-- SerializationBool::deserializeTextRaw (lines 369-374) is called when inserting Bool data via
-- the same format; also untested. The CRLF branch in deserializeTextEscaped (line 269) is reached
-- only when input_format_tsv_crlf_end_of_line = 1 is set, which no existing Bool test does.

-- serializeTextRaw: Bool column to TabSeparatedRaw output (lines 364-366)
SELECT true::Bool AS b FORMAT TabSeparatedRaw;
SELECT false::Bool AS b FORMAT TabSeparatedRaw;
SELECT materialize(true)::Bool AS b, materialize(false)::Bool AS b2 FORMAT TabSeparatedRaw;

-- deserializeTextRaw: Bool column from TabSeparatedRaw input (lines 369-374)
SELECT x FROM format(TabSeparatedRaw, 'x Bool', 'true');
SELECT x FROM format(TabSeparatedRaw, 'x Bool', 'false');
SELECT x FROM format(TabSeparatedRaw, 'x Bool', '1');
SELECT x FROM format(TabSeparatedRaw, 'x Bool', '0');

-- deserializeTextEscaped CRLF branch (line 269): Bool TSV input with Windows line endings
SELECT x FROM format(TabSeparated, 'x Bool', 'true\r\n') SETTINGS input_format_tsv_crlf_end_of_line = 1;
SELECT x FROM format(TabSeparated, 'x Bool', 'false\r\n') SETTINGS input_format_tsv_crlf_end_of_line = 1;
