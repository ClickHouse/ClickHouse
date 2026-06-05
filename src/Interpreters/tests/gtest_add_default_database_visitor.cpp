#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/IAST_fwd.h>

using namespace DB;

namespace
{

ASTPtr buildSelectFromTable(const String & table_name)
{
    auto select = make_intrusive<ASTSelectQuery>();

    auto select_list = make_intrusive<ASTExpressionList>();
    auto star = make_intrusive<ASTIdentifier>("*");
    select_list->children.push_back(star);
    select->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    auto tables_element = make_intrusive<ASTTablesInSelectQueryElement>();
    auto table_expression = make_intrusive<ASTTableExpression>();
    auto identifier = make_intrusive<ASTTableIdentifier>(table_name);

    table_expression->database_and_table_name = identifier;
    table_expression->children.push_back(identifier);

    tables_element->table_expression = table_expression;
    tables_element->children.push_back(table_expression);

    tables->children.push_back(tables_element);
    select->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    return select;
}

}

TEST(AddDefaultDatabaseVisitor, RecursiveWithFlagWithoutWithClauseDoesNotCrash)
{
    auto select = buildSelectFromTable("t");
    auto & select_typed = select->as<ASTSelectQuery &>();

    select_typed.recursive_with = true;
    ASSERT_EQ(select_typed.with(), nullptr);

    AddDefaultDatabaseVisitor visitor(getContext().context, "default_db");
    ASSERT_NO_FATAL_FAILURE(visitor.visit(select));

    auto * tables = select_typed.tables()->as<ASTTablesInSelectQuery>();
    ASSERT_NE(tables, nullptr);
    auto * tables_element = tables->children.at(0)->as<ASTTablesInSelectQueryElement>();
    ASSERT_NE(tables_element, nullptr);
    auto * table_expression = tables_element->table_expression->as<ASTTableExpression>();
    ASSERT_NE(table_expression, nullptr);
    auto * identifier = table_expression->database_and_table_name->as<ASTTableIdentifier>();
    ASSERT_NE(identifier, nullptr);
    EXPECT_TRUE(identifier->compound());
}

TEST(AddDefaultDatabaseVisitor, NoWithClauseDoesNotCrash)
{
    auto select = buildSelectFromTable("t");
    auto & select_typed = select->as<ASTSelectQuery &>();
    ASSERT_FALSE(select_typed.recursive_with);
    ASSERT_EQ(select_typed.with(), nullptr);

    AddDefaultDatabaseVisitor visitor(getContext().context, "default_db");
    ASSERT_NO_FATAL_FAILURE(visitor.visit(select));
}
