SET enable_analyzer = 0;

SELECT ((1, 2), (2, 3), (3, 4)) FORMAT TSVWithNames;
SELECT ((1, 2), (2, 3), (3, 4)) FORMAT TSVWithNames SETTINGS legacy_column_name_of_tuple_literal = 1;
