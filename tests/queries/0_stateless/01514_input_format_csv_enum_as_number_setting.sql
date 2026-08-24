DROP TABLE IF EXISTS table_with_enum_column_for_csv_insert;

CREATE TABLE table_with_enum_column_for_csv_insert (
    Id Int32,
    Value Enum('ef' = 1, 'es' = 2)
) ENGINE=Memory();

SET input_format_csv_enum_as_number = 1;

INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2

SELECT * FROM table_with_enum_column_for_csv_insert;

SET input_format_csv_enum_as_number = 0;

DROP TABLE IF EXISTS table_with_enum_column_for_csv_insert;
