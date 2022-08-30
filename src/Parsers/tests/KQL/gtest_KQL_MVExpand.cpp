#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_MVExpand, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "T | mv-expand c",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM T\n    ARRAY JOIN c\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand c, d",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM T\n    ARRAY JOIN\n        c,\n        d\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand c to typeof(bool)",
            "SELECT *\nFROM\n(\n    SELECT\n        * EXCEPT c_ali,\n        c_ali AS c\n    FROM\n    (\n        SELECT\n            * EXCEPT c,\n            accurateCastOrNull(c, 'Boolean') AS c_ali\n        FROM\n        (\n            SELECT *\n            FROM T\n            ARRAY JOIN c\n        )\n    )\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand b | mv-expand c",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM\n    (\n        SELECT *\n        FROM T\n        ARRAY JOIN b\n    )\n    ARRAY JOIN c\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand with_itemindex=index b, c, d",
            "SELECT *\nFROM\n(\n    SELECT\n        index,\n        *\n    FROM T\n    ARRAY JOIN\n        b,\n        c,\n        d,\n        range(0, arrayMax([length(b), length(c), length(d)])) AS index\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand array_concat(c,d)",
            "SELECT *\nFROM\n(\n    SELECT\n        *,\n        array_concat_\n    FROM T\n    ARRAY JOIN arrayConcat(c, d) AS array_concat_\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand x = c, y = d",
            "SELECT *\nFROM\n(\n    SELECT\n        *,\n        x,\n        y\n    FROM T\n    ARRAY JOIN\n        c AS x,\n        d AS y\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand xy = array_concat(c, d)",
            "SELECT *\nFROM\n(\n    SELECT\n        *,\n        xy\n    FROM T\n    ARRAY JOIN arrayConcat(c, d) AS xy\n)\nSETTINGS enable_unaligned_array_join = 1"
        },
        {
            "T | mv-expand with_itemindex=index c,d to typeof(bool)",
            "SELECT *\nFROM\n(\n    SELECT\n        * EXCEPT d_ali,\n        d_ali AS d\n    FROM\n    (\n        SELECT\n            * EXCEPT d,\n            accurateCastOrNull(d, 'Boolean') AS d_ali\n        FROM\n        (\n            SELECT\n                index,\n                *\n            FROM T\n            ARRAY JOIN\n                c,\n                d,\n                range(0, arrayMax([length(c), length(d)])) AS index\n        )\n    )\n)\nSETTINGS enable_unaligned_array_join = 1"
        }
})));
