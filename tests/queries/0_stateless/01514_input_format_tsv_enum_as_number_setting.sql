DROP TABLE IF EXISTS table_with_enum_column_for_tsv_insert;

CREATE TABLE table_with_enum_column_for_tsv_insert (
    Id Int32,
    Value Enum('ef' = 1, 'es' = 2)
) ENGINE=Memory();

SET input_format_tsv_enum_as_number = 1;

INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 102	2
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TabSeparatedRaw 103	1
SELECT * FROM table_with_enum_column_for_tsv_insert ORDER BY Id;

SET input_format_tsv_enum_as_number = 0;

DROP TABLE IF EXISTS table_with_enum_column_for_tsv_insert;
