#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_MVExpand, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "T | mv-expand c",
            "SELECT *\nFROM T\nARRAY JOIN c\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand c, d",
            "SELECT *\nFROM T\nARRAY JOIN\n    c,\n    d\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand c to typeof(bool)",
            "SELECT\n    * EXCEPT c_ali,\n    c_ali AS c\nFROM\n(\n    SELECT\n        * EXCEPT c,\n        accurateCastOrNull(toInt64OrNull(toString(c)), 'Boolean') AS c_ali\n    FROM\n    (\n        SELECT *\n        FROM T\n        ARRAY JOIN c\n    )\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand b | mv-expand c",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM T\n    ARRAY JOIN b\n    SETTINGS enable_unaligned_array_join = 1\n)\nARRAY JOIN c\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand with_itemindex=index b, c, d",
            "SELECT\n    index,\n    *\nFROM T\nARRAY JOIN\n    b,\n    c,\n    d,\n    range(0, arrayMax([length(b), length(c), length(d)])) AS index\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand array_concat(c,d)",
            "SELECT\n    *,\n    array_concat_\nFROM T\nARRAY JOIN arrayConcat(c, d) AS array_concat_\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand x = c, y = d",
            "SELECT\n    *,\n    x,\n    y\nFROM T\nARRAY JOIN\n    c AS x,\n    d AS y\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand xy = array_concat(c, d)",
            "SELECT\n    *,\n    xy\nFROM T\nARRAY JOIN arrayConcat(c, d) AS xy\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand with_itemindex=index c,d to typeof(bool)",
            "SELECT\n    * EXCEPT d_ali,\n    d_ali AS d\nFROM\n(\n    SELECT\n        * EXCEPT d,\n        accurateCastOrNull(toInt64OrNull(toString(d)), 'Boolean') AS d_ali\n    FROM\n    (\n        SELECT\n            index,\n            *\n        FROM T\n        ARRAY JOIN\n            c,\n            d,\n            range(0, arrayMax([length(c), length(d)])) AS index\n    )\n)\nSETTINGS enable_unaligned_array_join = 1"
        }
})));
