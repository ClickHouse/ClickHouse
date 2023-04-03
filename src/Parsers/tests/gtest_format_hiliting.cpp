#include <unordered_set>

#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>
#include <Common/StackTrace.h>

#include <utils/hilite_comparator/HiliteComparator.h>

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
    return hilite(s, DB::IAST::hilite_identifier);
}

String alias(const String & s)
{
    return hilite(s, DB::IAST::hilite_alias);
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


void compare(const String & query, const String & expected)
{
    using namespace DB;
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, 0, 0);

    WriteBufferFromOwnString write_buffer;
    IAST::FormatSettings settings(write_buffer, true);
    settings.hilite = true;
    ast->format(settings);

    ASSERT_PRED2(are_equal_with_hilites_removed, expected, write_buffer.str());
    ASSERT_PRED2(are_equal_with_hilites, expected, write_buffer.str());
}

TEST(FormatHiliting, SimpleSelect)
{
    String query = "select * from table";

    String expected = keyword("SELECT ") + "* " + keyword("FROM ") + identifier("table");

    compare(query, expected);
}

TEST(FormatHiliting, ASTWithElement)
{
    String query = "with alias as (select * from table) select * from table";

    String expected = keyword("WITH ") + alias("alias ") + keyword("AS ")
             + "(" + keyword("SELECT ") + "* " + keyword("FROM ") + identifier("table") + ") "
             + keyword("SELECT ") + "* " + keyword("FROM ") + identifier("table");

    compare(query, expected);
}

TEST(FormatHiliting, ASTWithAlias)
{
    String query = "select a + 1 as b, b";

    String expected = keyword("SELECT ") + identifier("a ") + op("+ ") + "1 " + keyword("AS ") + alias("b") + ", " + identifier("b");

    compare(query, expected);
}

TEST(FormatHiliting, ASTFunction)
{
    String query = "select * from view(select * from table)";

    String expected = keyword("SELECT ") + "* " + keyword("FROM ")
            + function("view(") + keyword("SELECT ") + "* " + keyword("FROM ") + identifier("table") + function(")");

    compare(query, expected);
}

TEST(FormatHiliting, ASTDictionaryAttributeDeclaration)
{
    String query = "CREATE DICTIONARY name (`Name` ClickHouseDataType DEFAULT '' EXPRESSION rand64() IS_OBJECT_ID)";

    String expected = keyword("CREATE DICTIONARY ") + "name "
            + "(`Name` " + function("ClickHouseDataType ")
            + keyword("DEFAULT ") + "'' "
            + keyword("EXPRESSION ") + function("rand64() ")
            + keyword("IS_OBJECT_ID") + ")";

    compare(query, expected);
}

TEST(FormatHiliting, ASTDictionaryClassSourceKeyword)
{
    String query = "CREATE DICTIONARY name (`Name` ClickHouseDataType DEFAULT '' EXPRESSION rand64() IS_OBJECT_ID) "
                        "SOURCE(FILE(PATH 'path'))";

    String expected = keyword("CREATE DICTIONARY ") + "name "
            + "(`Name` " + function("ClickHouseDataType ")
            + keyword("DEFAULT ") + "'' "
            + keyword("EXPRESSION ") + function("rand64() ")
            + keyword("IS_OBJECT_ID") + ") "
            + keyword("SOURCE") + "(" + keyword("FILE") + "(" + keyword("PATH ") + "'path'))";

    compare(query, expected);
}

TEST(FormatHiliting, ASTKillQueryQuery)
{
    String query = "KILL QUERY ON CLUSTER clustername WHERE user = 'username' SYNC";

    String expected = keyword("KILL QUERY ON CLUSTER ") + "clustername "
            + keyword("WHERE ") + identifier("user ") + op("= ") + "'username' "
            + keyword("SYNC");

    compare(query, expected);
}

TEST(FormatHiliting, ASTCreateQuery)
{
    String query = "CREATE TABLE name AS (SELECT *) COMMENT 'hello'";

    String expected = keyword("CREATE TABLE ") + "name " + keyword("AS (SELECT ") + "*" + keyword(") ")
            + keyword("COMMENT ") + "'hello'";

    compare(query, expected);
}
