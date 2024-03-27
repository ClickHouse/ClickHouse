SET allow_experimental_analyzer = 0;

SELECT ((1, 2), (2, 3), (3, 4)) FORMAT TSVWithNames;
SELECT ((1, 2), (2, 3), (3, 4)) FORMAT TSVWithNames SETTINGS legacy_column_name_of_literal = 1;
