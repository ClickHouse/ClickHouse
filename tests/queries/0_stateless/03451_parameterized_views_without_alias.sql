DROP TABLE IF EXISTS parameterized_view_without_renaming, parameterized_view_with_renaming;

CREATE VIEW parameterized_view_without_renaming AS
SELECT {test:Int32} * 2;
SELECT * FROM parameterized_view_without_renaming(test=42);

CREATE VIEW parameterized_view_with_renaming AS
SELECT {test:Int32} * 2 `result`;
SELECT * FROM parameterized_view_with_renaming(test=42);

DROP TABLE parameterized_view_without_renaming, parameterized_view_with_renaming;
