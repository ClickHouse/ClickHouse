#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTPredictQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <base/types.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ParserSelect, Basic)
{
    String input = "SELECT * FROM PREDICT(MODEL mdl, TABLE tbl);";

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, 0);

    EXPECT_TRUE(ast);
    // auto * tables_in_select = ast->as<ASTTablesInSelectQuery>();
    // EXPECT_TRUE(tables_in_select);

    // EXPECT_FALSE(tables_in_select->children.empty());
    // auto * tables_in_select_element = tables_in_select->children[0]->as<ASTTablesInSelectQueryElement>();
    // EXPECT_TRUE(tables_in_select_element);

    // EXPECT_TRUE(tables_in_select_element->table_expression);
    // auto * table_expression = tables_in_select_element->table_expression->as<ASTTableExpression>();
    // EXPECT_TRUE(table_expression);

    // EXPECT_TRUE(table_expression->table_function);
    // auto* function = table_expression->table_function->as<ASTFunction>();
    // EXPECT_TRUE(function);

    // EXPECT_EQ(function->name, "predict");
    // EXPECT_FALSE(function->arguments->children.empty());
    // auto * predict = function->arguments->children[0]->as<ASTPredictQuery>();
    // EXPECT_TRUE(predict);

    // EXPECT_EQ(predict->model_name->as<ASTIdentifier>()->name(), "mdl");
    // EXPECT_EQ(predict->table_name->as<ASTIdentifier>()->name(), "tbl");
}
