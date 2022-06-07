#include <Functions/selectors/parseNextAttribute.h>
#include <gtest/gtest.h>

using namespace DB;
using namespace std::literals::string_view_literals;

void checkAttributes(const char * begin, const char * end, const std::vector<Attribute> & expected)
{
    size_t ind = 0;
    Attribute attr;
    begin = parseNextAttribute(begin, end, attr);
    for (; begin != end; begin = parseNextAttribute(begin, end, attr))
    {
        EXPECT_EQ(attr, expected[ind++]);
        if (*begin == '>') {
            break;
        }
    }

    EXPECT_EQ(ind, expected.size());
}

TEST(parseNextAttribute, Unquoted)
{
    std::string s = R"( key1 = value1    key2=value2>)";

    std::vector<Attribute> expected = {
        {"key1"sv, "value1"},
        {"key2"sv, "value2"},
    };

    checkAttributes(s.data(), s.data() + s.size(), expected);
}

TEST(parseNextAttribute, SingleQuoted)
{
    std::string s = R"( key1 = 'va"<>lue1' key2='va"lue2' >)";

    std::vector<Attribute> expected = {
        {"key1"sv, "va\"<>lue1"},
        {"key2"sv, "va\"lue2"},
    };

    checkAttributes(s.data(), s.data() + s.size(), expected);
}

TEST(parseNextAttribute, DoubleQuoted)
{
    std::string s = R"( key1 = "va'lue1"
                        key2="va'<>lue2"
                        >)";

    std::vector<Attribute> expected = {
        {"key1"sv, "va'lue1"},
        {"key2"sv, "va'<>lue2"},
    };

    checkAttributes(s.data(), s.data() + s.size(), expected);
}
