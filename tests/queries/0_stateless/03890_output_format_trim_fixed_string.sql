SET output_format_write_statistics = 0;
SELECT toFixedString('John', 8) FORMAT TabSeparated;
SELECT toFixedString('John', 8) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;
SELECT toFixedString('John', 8) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparatedRaw;
SELECT toFixedString('John', 8) FORMAT TabSeparatedRaw;

SELECT toFixedString('abc\0def', 10) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;
SELECT toTypeName(toFixedString('John', 8)), length(toFixedString('John', 8)) FORMAT TabSeparated;
SELECT length(toFixedString('John', 8)) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;
SELECT toFixedString('', 8) FORMAT TabSeparated;
SELECT toFixedString('', 8) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;
SELECT toFixedString('\0John', 8) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;
SELECT toFixedString('John\0A', 8) SETTINGS output_format_trim_fixed_string = 1 FORMAT TabSeparated;

SELECT toFixedString('John', 8) AS s FORMAT JSONEachRow;
SELECT toFixedString('John', 8) AS s SETTINGS output_format_trim_fixed_string = 1 FORMAT JSONEachRow;

SELECT toFixedString('John', 8) SETTINGS output_format_trim_fixed_string = 1 FORMAT CSV;
SELECT toFixedString('John', 8) FORMAT CSV;

SELECT toFixedString('John', 8) AS s SETTINGS output_format_trim_fixed_string = 1 FORMAT Values;
SELECT toFixedString('John', 8) AS s FORMAT Values;

SELECT toFixedString('John', 8) AS s SETTINGS output_format_trim_fixed_string = 1 FORMAT XML;
SELECT toFixedString('John', 8) AS s FORMAT XML;

SELECT toFixedString('John', 8) AS s SETTINGS output_format_trim_fixed_string = 1 FORMAT Markdown;
SELECT toFixedString('John', 8) AS s FORMAT Markdown;

SELECT CAST(NULL, 'Nullable(FixedString(8))') SETTINGS format_csv_null_representation = 'null', output_format_trim_fixed_string = 1 FORMAT CSV;
-- Ensure NULL represented as \0 is not incorrectly trimmed when output_format_trim_fixed_string is enabled
SELECT CAST(NULL AS Nullable(FixedString(8))) SETTINGS format_csv_null_representation = '\0', output_format_trim_fixed_string = 1 FORMAT CSV;