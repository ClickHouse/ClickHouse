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
/// rebuilding the identifier through `name` and the constructor. A name supplied by a query
/// parameter is stored as an empty part whose substituting expression lives in the node's
/// children, so the rebuilt identifier carries no parameters and the constructor's
/// `chassert(!part.empty())` fails. The visitor must leave such a name alone.
///
/// This is a unit test rather than a stateless SQL test, per the carve-out of the
/// "default to SQL tests" rule: the behaviour is not observable from SQL on the build type
/// where the assertion is live. Reaching this rebuild site needs an explicit `UUID '...'`
/// clause on the table reference (`StorageID::assertNotEmpty` rejects the empty name first,
/// and `StorageID::empty` exempts a non-Nil UUID), and a separate pre-existing defect makes
/// any statement carrying that clause abort earlier on a debug binary: the formatter never
/// emits the clause while `ASTTableIdentifier::updateTreeHashImpl` hashes it, so the
/// format-parse-format check in `executeQueryImpl` raises `Inconsistent AST formatting`.
/// That check lives only in `executeQueryImpl`; parsing and running the visitor never enter
/// it, so this test exercises the site directly on every build type.
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
