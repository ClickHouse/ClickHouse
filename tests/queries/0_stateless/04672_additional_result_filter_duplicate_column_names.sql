-- A result header may legitimately contain several columns with the same name.
-- `additional_result_filter` is compiled against that header, and the compiled
-- expression only has an input for every *referenced* column, so the duplicates
-- must survive as pass-through columns of `FilterStep`, in the same order and
-- with the same values as without the setting.

SET enable_analyzer = 0;

SELECT number, number FROM numbers(3) FORMAT TSVWithNamesAndTypes SETTINGS additional_result_filter = 'number > 0';

SELECT number, number, number FROM numbers(3) FORMAT TSVWithNamesAndTypes SETTINGS additional_result_filter = 'number > 0';

-- The filter references a column that is not the first one: the referenced input becomes
-- the first output of the DAG and the untouched columns follow it.
SELECT number AS a, number, number AS b FROM numbers(3) FORMAT TSVWithNamesAndTypes SETTINGS additional_result_filter = 'b > 0';

SELECT number, number * 10 AS m, number FROM numbers(3) FORMAT TSVWithNamesAndTypes SETTINGS additional_result_filter = 'm > 0';

SELECT 1 AS x, 1 AS x FORMAT TSVWithNamesAndTypes SETTINGS additional_result_filter = 'x = 1';

-- With the Analyzer, a duplicate-named result header has never been supported together with
-- `additional_result_filter` — the same `ILLEGAL_COLUMN` is raised without this change as well.
SET enable_analyzer = 1;

SELECT number, number FROM numbers(3) FORMAT TSVWithNamesAndTypes SETTINGS additional_result_filter = 'number > 0'; -- { serverError ILLEGAL_COLUMN }
