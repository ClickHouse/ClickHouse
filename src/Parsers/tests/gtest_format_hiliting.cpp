#include <unordered_set>

#include <gtest/gtest.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Common/StackTrace.h>

std::string hilite(const std::string & s, const char * hilite_type)
{
    std::stringstream ss;
    ss << hilite_type;
    ss << s;
    ss << DB::IAST::hilite_none;
    return ss.str();
}

std::string keyword(const std::string & s)
{
    return hilite(s, DB::IAST::hilite_keyword);
}

std::string identifier(const std::string & s)
{
    return hilite(s, DB::IAST::hilite_identifier);
}

std::string alias(const std::string & s)
{
    return hilite(s, DB::IAST::hilite_alias);
}

std::string op(const std::string & s)
{
    return hilite(s, DB::IAST::hilite_operator);
}

std::string function(const std::string & s)
{
    return hilite(s, DB::IAST::hilite_function);
}

std::string substitution(const std::string & s)
{
    return hilite(s, DB::IAST::hilite_substitution);
}


bool are_equal_with_hilites(const std::string_view & left, const std::string_view & right)
{
    const char * left_it = left.begin();
    const char * right_it = right.begin();
    const char * left_hilite = DB::IAST::hilite_none;
    const char * right_hilite = DB::IAST::hilite_none;
    std::unordered_set<const char *> hilites = {DB::IAST::hilite_keyword, DB::IAST::hilite_identifier, DB::IAST::hilite_alias,
                                                DB::IAST::hilite_function, DB::IAST::hilite_operator, DB::IAST::hilite_substitution,
                                                DB::IAST::hilite_none};

    while (true)
    {
        // Consume hilites.
        bool changed_hilite = false;
        do
        {
            changed_hilite = false;
            for (const char * hilite : hilites)
            {
                if (std::string_view(left_it, left.end()).starts_with(hilite))
                {
                    left_hilite = hilite;
                    left_it += strlen(hilite);
                    changed_hilite = true;
                }
                if (std::string_view(right_it, right.end()).starts_with(hilite))
                {
                    right_hilite = hilite;
                    right_it += strlen(hilite);
                    changed_hilite = true;
                }
            }
        } while (changed_hilite);

        if (left_it == left.end() && right_it == right.end())
            return true;

        if (left_it == left.end() || right_it == right.end())
            return false;

        // Lookup one character.
        // Check characters match.
        if (*left_it != *right_it)
            return false;

        // Check hilites match if it's not a whitespace.
        if (!std::isspace(*left_it) && left_hilite != right_hilite)
            return false;

        // Consume one character.
        left_it++;
        right_it++;
    }
}

TEST(FormatHiliting, MetaTestAreEqualWithHilites)
{
    ASSERT_PRED2(are_equal_with_hilites, "", "");

    std::unordered_set<const char *> hilites = {DB::IAST::hilite_keyword, DB::IAST::hilite_identifier, DB::IAST::hilite_alias,
                                                DB::IAST::hilite_function, DB::IAST::hilite_operator, DB::IAST::hilite_substitution,
                                                DB::IAST::hilite_none};
    for (const char * hilite : hilites)
    {
        ASSERT_PRED2(are_equal_with_hilites, "", std::string_view(hilite));
        ASSERT_PRED2(are_equal_with_hilites, std::string_view(hilite), "");
    }

    {
        std::stringstream ss;
        ss << DB::IAST::hilite_none << "select" << DB::IAST::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites, ss.str(), "select");
    }

    {
        std::stringstream ss;
        ss << DB::IAST::hilite_none << "\n " << "sel" << DB::IAST::hilite_none << "ect" << DB::IAST::hilite_none;
        ASSERT_PRED2(are_equal_with_hilites, ss.str(), "\n select");
    }

    {
        std::stringstream left;
        left << DB::IAST::hilite_keyword << "keyword" << " long" << DB::IAST::hilite_none;
        std::stringstream right;
        right << DB::IAST::hilite_keyword << "keyword" << DB::IAST::hilite_none << " " << DB::IAST::hilite_keyword << "long";
        ASSERT_PRED2(are_equal_with_hilites, left.str(), right.str());
    }
}

void compare(const std::string & query, const std::stringstream & expected)
{
    using namespace DB;
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, 0, 0);

    WriteBufferFromOwnString write_buffer;
    IAST::FormatSettings settings{write_buffer, true};
    settings.hilite = true;
    ast->format(settings);

    ASSERT_PRED2(are_equal_with_hilites, expected.str(), write_buffer.str());
}

TEST(FormatHiliting, SimpleSelect)
{
    std::string query = "select * from table";

    std::stringstream expected;
    expected << keyword("SELECT ") << "* " << keyword("FROM ")
             << identifier("table");

    compare(query, expected);
}
