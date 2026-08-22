#include <gtest/gtest.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/MySQL/ASTDeclareConstraint.h>

using namespace DB;
using namespace DB::MySQLParser;

TEST(ParserConstraint, CheckConstraint)
{
    /// [CONSTRAINT [symbol]] CHECK (expr) [[NOT] ENFORCED]
    ParserDeclareConstraint p_constraint;

    String constraint_01 = "CONSTRAINT symbol_name CHECK col_01 = 1";
    ASTPtr ast_constraint_01 = parseQuery(p_constraint, constraint_01.data(), constraint_01.data() + constraint_01.size(), "", 0, 0, 0);
    EXPECT_EQ(ast_constraint_01->as<ASTDeclareConstraint>()->constraint_name, "symbol_name");
    auto * check_expression_01 = ast_constraint_01->as<ASTDeclareConstraint>()->check_expression->as<ASTFunction>();
    EXPECT_EQ(check_expression_01->name, "equals");
    EXPECT_EQ(check_expression_01->arguments->children[0]->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(check_expression_01->arguments->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);

    String constraint_02 = "CONSTRAINT CHECK col_01 = 1";
    ASTPtr ast_constraint_02 = parseQuery(p_constraint, constraint_02.data(), constraint_02.data() + constraint_02.size(), "", 0, 0, 0);
    EXPECT_EQ(ast_constraint_02->as<ASTDeclareConstraint>()->constraint_name, "");
    auto * check_expression_02 = ast_constraint_02->as<ASTDeclareConstraint>()->check_expression->as<ASTFunction>();
    EXPECT_EQ(check_expression_02->name, "equals");
    EXPECT_EQ(check_expression_02->arguments->children[0]->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(check_expression_02->arguments->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);

    String constraint_03 = "CHECK col_01 = 1";
    ASTPtr ast_constraint_03 = parseQuery(p_constraint, constraint_03.data(), constraint_03.data() + constraint_03.size(), "", 0, 0, 0);
    EXPECT_EQ(ast_constraint_03->as<ASTDeclareConstraint>()->constraint_name, "");
    auto * check_expression_03 = ast_constraint_03->as<ASTDeclareConstraint>()->check_expression->as<ASTFunction>();
    EXPECT_EQ(check_expression_03->name, "equals");
    EXPECT_EQ(check_expression_03->arguments->children[0]->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(check_expression_03->arguments->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);

    String constraint_04 = "CONSTRAINT CHECK col_01 = 1 ENFORCED";
    ASTPtr ast_constraint_04 = parseQuery(p_constraint, constraint_04.data(), constraint_04.data() + constraint_04.size(), "", 0, 0, 0);
    EXPECT_TRUE(ast_constraint_04->as<ASTDeclareConstraint>()->enforced);
    EXPECT_EQ(ast_constraint_04->as<ASTDeclareConstraint>()->constraint_name, "");
    auto * check_expression_04 = ast_constraint_04->as<ASTDeclareConstraint>()->check_expression->as<ASTFunction>();
    EXPECT_EQ(check_expression_04->name, "equals");
    EXPECT_EQ(check_expression_04->arguments->children[0]->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(check_expression_04->arguments->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);

    String constraint_05 = "CONSTRAINT CHECK col_01 = 1 NOT ENFORCED";
    ASTPtr ast_constraint_05 = parseQuery(p_constraint, constraint_05.data(), constraint_05.data() + constraint_05.size(), "", 0, 0, 0);
    EXPECT_FALSE(ast_constraint_05->as<ASTDeclareConstraint>()->enforced);
    EXPECT_EQ(ast_constraint_05->as<ASTDeclareConstraint>()->constraint_name, "");
    auto * check_expression_05 = ast_constraint_05->as<ASTDeclareConstraint>()->check_expression->as<ASTFunction>();
    EXPECT_EQ(check_expression_05->name, "equals");
    EXPECT_EQ(check_expression_05->arguments->children[0]->as<ASTIdentifier>()->name(), "col_01");
    EXPECT_EQ(check_expression_05->arguments->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 1);
}
