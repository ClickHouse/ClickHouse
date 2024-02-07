CREATE TABLE 02983_fixedstring_map (`m` Map(LowCardinality(FixedString(8)), Date32)) ENGINE = Memory;
INSERT INTO 02983_fixedstring_map VALUES (map('a', 1, 'b', 2)), (map('c', 3, 'd', 4));
SELECT mapContains(m, toFixedString(toFixedString(materialize(toLowCardinality('a')), 1), 1)) FROM 02983_fixedstring_map settings allow_experimental_analyzer=1, optimize_functions_to_subcolumns=1;
