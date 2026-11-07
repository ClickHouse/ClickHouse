#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

template <typename T, typename U>
void checkString(const T & str, U manip, const std::string & expected)
{
    WriteBufferFromOwnString buf;

    buf << manip << str;
    EXPECT_EQ(expected, buf.str()) << "str type:" << typeid(str).name();
}

TEST(OperatorsManipTest, EscapingTest)
{
    checkString("Hello 'world'", escape, "Hello \\'world\\'");
    checkString("Hello \\world\\", escape, "Hello \\\\world\\\\"); // NOLINT

    std::string s1 = "Hello 'world'";
    checkString(s1, escape, "Hello \\'world\\'");
    std::string s2 = "Hello \\world\\";
    checkString(s2, escape, "Hello \\\\world\\\\"); // NOLINT

    std::string_view sv1 = s1;
    checkString(sv1, escape, "Hello \\'world\\'");
    std::string_view sv2 = s2;
    checkString(sv2, escape, "Hello \\\\world\\\\"); // NOLINT

    std::string_view sr1 = s1;
    checkString(sr1, escape, "Hello \\'world\\'");
    std::string_view sr2 = s2;
    checkString(sr2, escape, "Hello \\\\world\\\\"); // NOLINT
}

TEST(OperatorsManipTest, QuouteTest)
{
    checkString("Hello 'world'", quote, "'Hello \\'world\\''");

    std::string s1 = "Hello 'world'";
    checkString(s1, quote, "'Hello \\'world\\''");

    std::string_view sv1 = s1;
    checkString(sv1, quote, "'Hello \\'world\\''");

    std::string_view sr1 = s1;
    checkString(sr1, quote, "'Hello \\'world\\''");
}

TEST(OperatorsManipTest, DoubleQuouteTest)
{
    checkString("Hello 'world'", double_quote, "\"Hello 'world'\"");

    std::string s1 = "Hello 'world'";
    checkString(s1, double_quote, "\"Hello 'world'\"");

    std::string_view sv1 = s1;
    checkString(sv1, double_quote, "\"Hello 'world'\"");

    std::string_view sr1 = s1;
    checkString(sr1, double_quote, "\"Hello 'world'\"");
}

TEST(OperatorsManipTest, binary)
{
    checkString("Hello", binary, "\x5Hello");

    std::string s1 = "Hello";
    checkString(s1, binary, "\x5Hello");

    std::string_view sv1 = s1;
    checkString(sv1, binary, "\x5Hello");

    std::string_view sr1 = s1;
    checkString(sr1, binary, "\x5Hello");
}

TEST(WriteHelpersTest, MySQLStringLiteral)
{
    FormatSettings settings;
    settings.values.use_mysql_compatible_escaping = true;

    WriteBufferFromOwnString quoted;
    writeStringForValues("Hello 'world'", quoted, settings);
    EXPECT_EQ("'Hello ''world'''", quoted.str());

    std::string value = "3c6f395fc759";
    value += static_cast<char>(0x45);
    value += static_cast<char>(0x00);
    value += static_cast<char>(0x0C);
    value += static_cast<char>(0x5C);
    value += static_cast<char>(0x8F);
    value += "18";

    WriteBufferFromOwnString binary_value;
    writeStringForValues(value, binary_value, settings);
    EXPECT_EQ("X'33633666333935666337353945000C5C8F3138'", binary_value.str());
}
