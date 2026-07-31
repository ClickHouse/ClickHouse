#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

ASTPtr parseSelect(const String & query)
{
    ParserSelectQuery parser;
    return parseQuery(parser, query, 0, 0, 0);
}

/// The single table identifier of `SELECT ... FROM <one table>`.
const ASTTableIdentifier & tableIdentifier(const ASTPtr & ast)
{
    const auto & select = ast->as<ASTSelectQuery &>();
    const auto & tables = select.tables()->as<ASTTablesInSelectQuery &>();
    const auto & element = tables.children.at(0)->as<ASTTablesInSelectQueryElement &>();
    const auto & table_expression = element.table_expression->as<ASTTableExpression &>();
    return table_expression.database_and_table_name->as<ASTTableIdentifier &>();
}

}

/// `AddDefaultDatabaseVisitor` qualifies a one-part table name with the current database by
/// rebuilding the identifier through `name` and the constructor, which drops the query
/// parameters, so a parameterized name would re-enter the constructor with an empty part and
/// trip `chassert(!part.empty())`. The visitor must leave such a name alone.
///
/// Driven directly rather than through SQL: reaching this rebuild needs an explicit
/// `UUID '...'` clause (`StorageID::empty` exempts a non-Nil UUID from `assertNotEmpty`), and
/// on a debug build any statement carrying that clause hits a separate pre-existing formatter
/// defect first. Sanitizer builds keep `chassert` but define `NDEBUG`, so they would reach the
/// assertion from SQL; a debug stress job would not, and `no-debug` is ignored there.
TEST(AddDefaultDatabaseVisitor, ParameterizedTableNameWithUUIDIsLeftUnqualified)
{
    const auto & context = getContext().context;

    ASTPtr ast = parseSelect("SELECT k FROM {ptab:Identifier} UUID '01234567-89ab-cdef-0123-456789abcdef'");

    /// The premise the rebuild site rests on. If any of these stops holding, the test below
    /// would pass without exercising anything, so assert them rather than assume them.
    {
        const auto & identifier = tableIdentifier(ast);
        ASSERT_TRUE(identifier.isParam());
        ASSERT_EQ(identifier.name_parts.size(), 1u);
        ASSERT_TRUE(identifier.name_parts[0].empty());
        ASSERT_TRUE(identifier.has_uuid);
        ASSERT_NE(identifier.uuid, UUIDHelpers::Nil);
    }

    AddDefaultDatabaseVisitor(context, "some_db").visit(ast);

    const auto & identifier = tableIdentifier(ast);
    EXPECT_TRUE(identifier.isParam());
    EXPECT_EQ(identifier.name_parts.size(), 1u);
    EXPECT_TRUE(identifier.name_parts[0].empty());
}

/// Must-not-regress control: an ordinary one-part name is still qualified with the current
/// database, so the guard above is not over-broad.
TEST(AddDefaultDatabaseVisitor, OrdinaryTableNameIsQualified)
{
    const auto & context = getContext().context;

    ASTPtr ast = parseSelect("SELECT k FROM t");

    {
        const auto & identifier = tableIdentifier(ast);
        ASSERT_FALSE(identifier.isParam());
        ASSERT_EQ(identifier.name(), "t");
    }

    AddDefaultDatabaseVisitor(context, "some_db").visit(ast);

    const auto & identifier = tableIdentifier(ast);
    EXPECT_FALSE(identifier.isParam());
    EXPECT_EQ(identifier.name(), "some_db.t");
    ASSERT_EQ(identifier.name_parts.size(), 2u);
    EXPECT_EQ(identifier.name_parts[0], "some_db");
    EXPECT_EQ(identifier.name_parts[1], "t");
}
