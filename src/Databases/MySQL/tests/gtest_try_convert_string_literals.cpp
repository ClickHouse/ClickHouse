#include <gtest/gtest.h>

#include <Databases/MySQL/tryConvertStringLiterals.h>

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

class ConvertStringLiteralsTest : public ::testing::TestWithParam<TestCase>
{
};

TEST_P(ConvertStringLiteralsTest, escape)
{
    const auto & [query, expected, ok] = GetParam();
    String actual = query;
    bool res = tryConvertStringLiterals(actual);
    EXPECT_EQ(ok, res);
    EXPECT_EQ(expected, actual);
}

INSTANTIATE_TEST_SUITE_P(MaterializedMySQL, ConvertStringLiteralsTest, ::testing::ValuesIn(std::initializer_list<TestCase>{
    {
        "",
        "",
        false
    },
    {
        "'",
        "'",
        false
    },
    {
        "''",
        "''",
        false
    },
    {
        "'''",
        "'''",
        false
    },
    {
        "''err'",
        "''err'",
        false
    },
    {
        "\"",
        "\"",
        false
    },
    {
        "\"\"",
        "''",
        true
    },
    {
        "`",
        "`",
        false
    },
    {
        "``",
        "``",
        false
    },
    {
        "SELECT \"st'r\", 'st\"r';",
        "SELECT 'st\\'r', 'st\"r';",
        true
    },
    {
        "SELECT * FROM `db`.`table` WHERE id = '1'",
        "SELECT * FROM `db`.`table` WHERE id = '1'",
        false
    },
    {
        "CREATE TABLE gb2312.`道渠` (`id` int NOT NULL DEFAULT \"0\",",
        "CREATE TABLE gb2312.`道渠` (`id` int NOT NULL DEFAULT '0',",
        true
    },
    {
        "SELECT _latin1'\x4D\x79\x53\x51\x4C';",
        "SELECT 'MySQL';",
        true
    },
    {
        "SELECT _latin1'\x4D\xFC\x6C\x6C\x65\x72', _utf8mb4\"\", _latin1 '', skip, 'skip', _gb2312'\xc4\xe3\xba\xc3', \"你好\", '你好', _utf8mb4'你好' FROM table",
        "SELECT 'Müller', '', '', skip, 'skip', '你好', '你好', '你好', '你好' FROM table",
        true
    },
    {
        "_utf8'', _utf8\"\", skip _utf8mb4, _utf8mb4\"\" _utf8mb3 \"\" _latin1 skip '', _binary skip",
        "_utf8'', _utf8\'\', skip _utf8mb4, '' '' _latin1 skip '', _binary skip",
        true
    },
    {
        "SELECT _binary;",
        "SELECT _binary;",
        false
    },
    {
        "SELECT _utf8mb4;_utf8mb4",
        "SELECT _utf8mb4;_utf8mb4",
        false
    },
    {
        "SELECT _utf8mb4'';_utf8mb4",
        "SELECT '';_utf8mb4",
        true
    },
    {
        "SELECT _utf8mb4'a\"b\nc\\'d';_utf8mb4'",
        "SELECT 'a\"b\\nc\\'d';_utf8mb4'",
        true
    },
    { /// TODO
        "SELECT _binary'abc';",
        "SELECT _binary'abc';",
        false
    },
    { /// TODO
        "SELECT _latin1 X'4D7953514C';",
        "SELECT _latin1 X'4D7953514C';",
        false
    },
    { /// TODO
        "SELECT _latin1 b'1000001';",
        "SELECT _latin1 b'1000001';",
        false
    },
}));
