#include <gtest/gtest.h>

#include <Databases/MySQL/tryQuoteUnrecognizedTokens.h>

using namespace DB;

namespace
{

struct TestCase
{
    String query;
    String res;
    bool ok;

    TestCase(
        const String & query_,
        const String & res_,
        bool ok_)
        : query(query_)
        , res(res_)
        , ok(ok_)
    {
    }
};

std::ostream & operator<<(std::ostream & ostr, const TestCase & test_case)
{
    return ostr << '"' << test_case.query << "\" -> \"" << test_case.res << "\" ok:" << test_case.ok;
}

}

class QuoteUnrecognizedTokensTest : public ::testing::TestWithParam<TestCase>
{
};

TEST_P(QuoteUnrecognizedTokensTest, escape)
{
    const auto & [query, expected, ok] = GetParam();
    String actual = query;
    bool res = tryQuoteUnrecognizedTokens(actual);
    EXPECT_EQ(ok, res);
    EXPECT_EQ(expected, actual);
}

INSTANTIATE_TEST_SUITE_P(MaterializedMySQL, QuoteUnrecognizedTokensTest, ::testing::ValuesIn(std::initializer_list<TestCase>{
    {
        "",
        "",
        false
    },
    {
        "test '\"`",
        "test '\"`",
        false
    },
    {
        "SELECT * FROM db.`table`",
        "SELECT * FROM db.`table`",
        false
    },
    {
        "道渠",
        "`道渠`",
        true
    },
    {
        "道",
        "`道`",
        true
    },
    {
        "道道(skip) 道(",
        "`道道`(skip) `道`(",
        true
    },
    {
        "`道渠`",
        "`道渠`",
        false
    },
    {
        "'道'",
        "'道'",
        false
    },
    {
        "\"道\"",
        "\"道\"",
        false
    },
    {
        "` 道 test 渠 `",
        "` 道 test 渠 `",
        false
    },
    {
        "skip 道 skip 123",
        "skip `道` skip 123",
        true
    },
    {
        "skip 123 `道` skip",
        "skip 123 `道` skip",
        false
    },
    {
        "skip `道 skip 123",
        "skip `道 skip 123",
        false
    },
    {
        "skip test道 skip",
        "skip `test道` skip",
        true
    },
    {
        "test道2test",
        "`test道2test`",
        true
    },
    {
        "skip test道2test 123",
        "skip `test道2test` 123",
        true
    },
    {
        "skip 您a您a您a a您a您a您a 1您2您3您4 skip",
        "skip `您a您a您a` `a您a您a您a` `1您2您3您4` skip",
        true
    },
    {
        "skip 您a 您a您a b您2您c您4 skip",
        "skip `您a` `您a您a` `b您2您c您4` skip",
        true
    },
    {
        "123您a skip 56_您a 您a2 b_您2_您c123您_a4 skip",
        "`123您a` skip `56_您a` `您a2` `b_您2_您c123您_a4` skip",
        true
    },
    {
        "_您_ 123 skip 56_您_您_您_您_您_您_您_您_您_a 您a2 abc 123_您_您_321 a1b2c3 aaaaa您您_a4 skip",
        "`_您_` 123 skip `56_您_您_您_您_您_您_您_您_您_a` `您a2` abc `123_您_您_321` a1b2c3 `aaaaa您您_a4` skip",
        true
    },
    {
        "TABLE 您2 您(",
        "TABLE `您2` `您`(",
        true
    },
    {
        "TABLE 您.a您2(日2日2 INT",
        "TABLE `您`.`a您2`(`日2日2` INT",
        true
    },
    {
        "TABLE 您$.a_您2a_($日2日_2 INT, 您Hi好 a您b好c)",
        "TABLE `您`$.`a_您2a_`($`日2日_2` INT, `您Hi好` `a您b好c`)",
        true
    },
    {
        "TABLE 您a日.您a您a您a(test INT",
        "TABLE `您a日`.`您a您a您a`(test INT",
        true
    },
    {
        "TABLE 您a日.您a您a您a(Hi您Hi好Hi INT",
        "TABLE `您a日`.`您a您a您a`(`Hi您Hi好Hi` INT",
        true
    },
    {
        "--TABLE 您a日.您a您a您a(test INT",
        "--TABLE 您a日.您a您a您a(test INT",
        false
    },
    {
        "--您a日.您a您a您a(\n您Hi好",
        "--您a日.您a您a您a(\n`您Hi好`",
        true
    },
    {
        " /* TABLE 您a日.您a您a您a(test INT",
        " /* TABLE 您a日.您a您a您a(test INT",
        false
    },
    {
        "/*您a日.您a您a您a(*/\n您Hi好",
        "/*您a日.您a您a您a(*/\n`您Hi好`",
        true
    },
    {
        " 您a日.您您aa您a /* 您a日.您a您a您a */ a您a日a.a您您您a",
        " `您a日`.`您您aa您a` /* 您a日.您a您a您a */ `a您a日a`.`a您您您a`",
        true
    },
    //{ TODO
    //    "TABLE 您2.您a您a您a(test INT",
    //    "TABLE `您2`.`您a您a您a`(test INT",
    //    true
    //},
    {
        "skip 您a您a您a skip",
        "skip `您a您a您a` skip",
        true
    },
    {
        "test 您a2您3a您a 4 again",
        "test `您a2您3a您a` 4 again",
        true
    },
    {
        "CREATE TABLE db.`道渠`",
        "CREATE TABLE db.`道渠`",
        false
    },
    {
        "CREATE TABLE db.`道渠",
        "CREATE TABLE db.`道渠",
        false
    },
    {
        "CREATE TABLE db.道渠",
        "CREATE TABLE db.`道渠`",
        true
    },
    {
        "CREATE TABLE db.     道渠",
        "CREATE TABLE db.     `道渠`",
        true
    },
    {
        R"sql(
        CREATE TABLE gb2312.`道渠` (   `id` int NOT NULL,
            您 INT,
            道渠 DATETIME,
            您test INT, test您 INT, test您test INT,
            道渠test INT, test道渠 INT, test道渠test INT,
            您_ INT, _您 INT, _您_ INT,
            您您__ INT, __您您 INT, __您您__ INT,
            您2 INT, 2您 INT, 2您2 INT,
            您您22 INT, 22您您 INT, 22您您22 INT,
            您_2 INT, _2您 INT, _2您_2 INT, _2您2_ INT, 2_您_2 INT,
            您您__22 INT, __22您您 INT, __22您您__22 INT, __22您您22__ INT, 22__您您__22 INT,
            您2_ INT, 2_您 INT, 2_您2_ INT,
            您您22__ INT, 22__您您 INT, 22__您您22__ INT,
            您_test INT, _test您 INT, _test您_test INT, _test您test_ INT, test_您test_ INT, test_您_test INT,
            您您_test INT, _test您您 INT, _test您您_test INT, _test您您test_ INT, test_您您test_ INT, test_您您_test INT,
            您test3 INT, test3您 INT, test3您test3 INT, test3您3test INT,
            您您test3 INT, test3您您 INT, test3您您test3 INT, test3您您3test  INT,
            您3test INT, 3test您 INT, 3test您3test INT, 3test您test3 INT,
            您您3test INT, 3test您您 INT, 3test您您3test INT, 3test您您test3 INT,
            您_test4 INT, _test4您 INT, _test4您_test4 INT, test4_您_test4 INT, _test4您4test_ INT, _test4您test4_ INT,
            您您_test4 INT, _test4您您 INT, _test4您您_test4 INT, test4_您您_test4 INT, _test4您您4test_ INT, _test4您您test4_ INT,
            您_5test INT, _5test您 INT, _5test您_5test INT, 5test_您_test5 INT, _4test您test4_ INT,
            test_日期     varchar(256), test_道_2     varchar(256) NOT NULL   ,
            test_道渠您_3
                BIGINT  NOT NULL,
            道您3_test INT,
            PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=gb2312;
        )sql",
        R"sql(
        CREATE TABLE gb2312.`道渠` (   `id` int NOT NULL,
            `您` INT,
            `道渠` DATETIME,
            `您test` INT, `test您` INT, `test您test` INT,
            `道渠test` INT, `test道渠` INT, `test道渠test` INT,
            `您_` INT, `_您` INT, `_您_` INT,
            `您您__` INT, `__您您` INT, `__您您__` INT,
            `您2` INT, `2您` INT, `2您2` INT,
            `您您22` INT, `22您您` INT, `22您您22` INT,
            `您_2` INT, `_2您` INT, `_2您_2` INT, `_2您2_` INT, `2_您_2` INT,
            `您您__22` INT, `__22您您` INT, `__22您您__22` INT, `__22您您22__` INT, `22__您您__22` INT,
            `您2_` INT, `2_您` INT, `2_您2_` INT,
            `您您22__` INT, `22__您您` INT, `22__您您22__` INT,
            `您_test` INT, `_test您` INT, `_test您_test` INT, `_test您test_` INT, `test_您test_` INT, `test_您_test` INT,
            `您您_test` INT, `_test您您` INT, `_test您您_test` INT, `_test您您test_` INT, `test_您您test_` INT, `test_您您_test` INT,
            `您test3` INT, `test3您` INT, `test3您test3` INT, `test3您3test` INT,
            `您您test3` INT, `test3您您` INT, `test3您您test3` INT, `test3您您3test`  INT,
            `您3test` INT, `3test您` INT, `3test您3test` INT, `3test您test3` INT,
            `您您3test` INT, `3test您您` INT, `3test您您3test` INT, `3test您您test3` INT,
            `您_test4` INT, `_test4您` INT, `_test4您_test4` INT, `test4_您_test4` INT, `_test4您4test_` INT, `_test4您test4_` INT,
            `您您_test4` INT, `_test4您您` INT, `_test4您您_test4` INT, `test4_您您_test4` INT, `_test4您您4test_` INT, `_test4您您test4_` INT,
            `您_5test` INT, `_5test您` INT, `_5test您_5test` INT, `5test_您_test5` INT, `_4test您test4_` INT,
            `test_日期`     varchar(256), `test_道_2`     varchar(256) NOT NULL   ,
            `test_道渠您_3`
                BIGINT  NOT NULL,
            `道您3_test` INT,
            PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=gb2312;
        )sql",
        true
    },
}));
