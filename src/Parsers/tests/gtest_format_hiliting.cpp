#include <unordered_set>

#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/HiliteComparator/HiliteComparator.h>
#include <gtest/gtest.h>
#include <Common/quoteString.h>


using namespace DB;

String hilite(const String & s, const char * hilite_type)
{
    return hilite_type + s + DB::IAST::hilite_none;
}

String keyword(const String & s)
{
    return hilite(s, DB::IAST::hilite_keyword);
}

String identifier(const String & s)
{
    return hilite(backQuoteIfNeed(s), DB::IAST::hilite_identifier);
}

String alias(const String & s)
{
    return hilite(backQuoteIfNeed(s), DB::IAST::hilite_alias);
}

String op(const String & s)
{
    return hilite(s, DB::IAST::hilite_operator);
}

String function(const String & s)
{
    return hilite(s, DB::IAST::hilite_function);
}

String substitution(const String & s)
{
    return hilite(s, DB::IAST::hilite_substitution);
}


void compare(const String & expected, const String & query)
{
    using namespace DB;
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, 0, 0, 0);

    WriteBufferFromOwnString write_buffer;
    IAST::FormatSettings settings(true, true);
    ast->format(write_buffer, settings);

    ASSERT_PRED2(HiliteComparator::are_equal_with_hilites_removed, expected, write_buffer.str());
    ASSERT_PRED2(HiliteComparator::are_equal_with_hilites_and_end_without_hilite, expected, write_buffer.str());
}

const std::vector<std::pair<std::string, std::string>> expected_and_query_pairs = {
    // Simple select
    {
        keyword("SELECT") + " * " + keyword("FROM") + " " + identifier("table"),
        "select * from `table`"
    },

    // ASTWithElement
    {
        keyword("WITH ") + alias("alias ") + " " + keyword("AS")
            + " (" + keyword("SELECT") + " * " + keyword("FROM") + " " + identifier("table") + ") "
            + keyword("SELECT") + " * " + keyword("FROM") + " " + identifier("table"),
        "with `alias ` as (select * from `table`) select * from `table`"
    },

    // ASTWithAlias
    {
        keyword("SELECT") + " " + identifier("a") + " " + op("+") + " 1 " + keyword("AS") + " " + alias("b") + ", " + identifier("b"),
        "select a + 1 as b, b"
    },

    // ASTFunction
    {
        keyword("SELECT ") + "* " + keyword("FROM ")
            + function("view(") + keyword("SELECT") + " * " + keyword("FROM ") + identifier("table") + function(")"),
        "select * from view(select * from `table`)"
    },

    // ASTDictionaryAttributeDeclaration
    {
        keyword("CREATE DICTIONARY ") + identifier("name") + " "
            + "(`Name` " + function("ClickHouseDataType")
            + keyword(" DEFAULT") + " '' "
            + keyword("EXPRESSION") + " " + function("rand64()") + " "
            + keyword("IS_OBJECT_ID") + ")",
        "CREATE DICTIONARY name (`Name` ClickHouseDataType DEFAULT '' EXPRESSION rand64() IS_OBJECT_ID)"
    },

    // ASTDictionary, SOURCE keyword
    {
        keyword("CREATE DICTIONARY ") + identifier("name") + " "
            + "(`Name`" + " " + function("ClickHouseDataType ")
            + keyword("DEFAULT") + " '' "
            + keyword("EXPRESSION") + " " + function("rand64()") + " "
            + keyword("IS_OBJECT_ID") + ") "
            + keyword("SOURCE") + "(" + keyword("FILE") + "(" + keyword("PATH") + " 'path'))",
        "CREATE DICTIONARY name (`Name` ClickHouseDataType DEFAULT '' EXPRESSION rand64() IS_OBJECT_ID) "
        "SOURCE(FILE(PATH 'path'))"
    },

    // ASTKillQueryQuery
    {
        keyword("KILL QUERY ON CLUSTER") + " clustername "
            + keyword("WHERE") + " " + identifier("user") + op(" = ") + "'username' "
            + keyword("SYNC"),
        "KILL QUERY ON CLUSTER clustername WHERE user = 'username' SYNC"
    },

    // ASTCreateQuery
    {
        keyword("CREATE TABLE ") + identifier("name") + " " + keyword("AS (SELECT") + " *" + keyword(")") + " "
            + keyword("COMMENT") + " 'hello'",
        "CREATE TABLE name AS (SELECT *) COMMENT 'hello'"
    },
};


TEST(FormatHiliting, Queries)
{
    for (const auto & [expected, query] : expected_and_query_pairs)
        compare(expected, query);
}
