#include <Parsers/ASTCastTarget.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTUDTReference.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <vector>


namespace
{
using namespace DB;

ASTPtr parseExpression(const String & text)
{
    ParserExpression parser;
    return parseQuery(parser, text, "user-defined type CAST target test", 0, 150, 0);
}

const ASTFunction * getCastFunction(const ASTPtr & ast)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || function->name != "CAST")
        return nullptr;
    return function;
}

void expectStringLiteralTarget(const String & expression, const String & expected_target)
{
    const auto ast = parseExpression(expression);
    const auto * function = getCastFunction(ast);
    ASSERT_NE(function, nullptr) << expression;
    EXPECT_EQ(function->tryGetStructuredCastTarget(), nullptr) << expression;
    ASSERT_NE(function->arguments, nullptr) << expression;
    ASSERT_EQ(function->arguments->children.size(), 2) << expression;

    const auto * literal = function->arguments->children[1]->as<ASTLiteral>();
    ASSERT_NE(literal, nullptr) << expression;
    ASSERT_EQ(literal->value.getType(), Field::Types::String) << expression;
    EXPECT_EQ(literal->value.safeGet<String>(), expected_target) << expression;
}

void expectPostfixJSONPath(const String & expression, const std::vector<String> & path)
{
    const auto ast = parseExpression(expression);
    ASTPtr current = ast;
    for (auto component = path.rbegin(); component != path.rend(); ++component)
    {
        const auto * tuple_element = current->as<ASTFunction>();
        ASSERT_NE(tuple_element, nullptr) << expression;
        ASSERT_EQ(tuple_element->name, "tupleElement") << expression;
        ASSERT_NE(tuple_element->arguments, nullptr) << expression;
        ASSERT_EQ(tuple_element->arguments->children.size(), 2) << expression;

        const auto * field = tuple_element->arguments->children[1]->as<ASTLiteral>();
        ASSERT_NE(field, nullptr) << expression;
        ASSERT_EQ(field->value.getType(), Field::Types::String) << expression;
        EXPECT_EQ(field->value.safeGet<String>(), *component) << expression;
        current = tuple_element->arguments->children[0];
    }

    const auto * cast = getCastFunction(current);
    ASSERT_NE(cast, nullptr) << expression;
    EXPECT_EQ(cast->tryGetStructuredCastTarget(), nullptr) << expression;
    ASSERT_NE(cast->arguments, nullptr) << expression;
    ASSERT_EQ(cast->arguments->children.size(), 2) << expression;
    const auto * target = cast->arguments->children[1]->as<ASTLiteral>();
    ASSERT_NE(target, nullptr) << expression;
    ASSERT_EQ(target->value.getType(), Field::Types::String) << expression;
    EXPECT_EQ(target->value.safeGet<String>(), "JSON") << expression;
}

}

TEST(UDTCastTarget, QualifiedTargetSurvivesEveryTypeSyntaxPath)
{
    const std::vector<String> expressions = {
        "CAST(1 AS app.UserId)",
        "CAST(1 AS Array(app.UserId))",
        "1::app.UserId",
        "plus(1, 2)::app.UserId",
    };

    for (const auto & expression : expressions)
    {
        const auto ast = parseExpression(expression);
        const auto * function = getCastFunction(ast);
        ASSERT_NE(function, nullptr) << expression;

        const auto * target = function->tryGetStructuredCastTarget();
        ASSERT_NE(target, nullptr) << expression;
        EXPECT_NE(target->getType(), nullptr) << expression;
    }
}

TEST(UDTCastTarget, BuiltInTargetsKeepStringLiteralShape)
{
    expectStringLiteralTarget("CAST(1 AS UInt64)", "UInt64");
    expectStringLiteralTarget("CAST(1 AS Array(UInt8))", "Array(UInt8)");
    expectStringLiteralTarget("1::UInt64", "UInt64");
    expectStringLiteralTarget("plus(1, 2)::UInt64", "UInt64");
    expectStringLiteralTarget("CAST(1, 'UInt64')", "UInt64");
    expectStringLiteralTarget("CAST(1, 'app.UserId')", "app.UserId");
    expectStringLiteralTarget("CAST(1, 'Array(app.UserId)')", "Array(app.UserId)");
    expectStringLiteralTarget("CAST(1, 'Enum8(\\'a.b\\' = 1)')", "Enum8('a.b' = 1)");

    EXPECT_EQ(parseExpression("CAST(1 AS UInt64)")->formatWithSecretsOneLine(), "CAST(1, 'UInt64')");
}

TEST(UDTCastTarget, PostfixJSONPathRemainsOutsideTheCastTarget)
{
    expectPostfixJSONPath("'{}'::JSON.a", {"a"});
    expectPostfixJSONPath("materialize('{}')::JSON.a.b", {"a", "b"});

    const auto quoted_database = parseExpression("1::`JSON`.a");
    const auto * cast = getCastFunction(quoted_database);
    ASSERT_NE(cast, nullptr);
    EXPECT_NE(cast->tryGetStructuredCastTarget(), nullptr);
}

TEST(UDTCastTarget, CloneHashFormatAndColumnNameRoundTrip)
{
    const String expression = "CAST(1 AS Array(`analytics.db`.`User.Type`(16, 'external')))";
    const auto ast = parseExpression(expression);
    const auto * function = getCastFunction(ast);
    ASSERT_NE(function, nullptr);
    ASSERT_NE(function->tryGetStructuredCastTarget(), nullptr);

    const String formatted = ast->formatWithSecretsOneLine();
    EXPECT_EQ(formatted, expression);
    EXPECT_EQ(ast->getColumnName(), formatted);

    const auto cloned = ast->clone();
    const auto * cloned_function = getCastFunction(cloned);
    ASSERT_NE(cloned_function, nullptr);
    ASSERT_NE(cloned_function->tryGetStructuredCastTarget(), nullptr);
    EXPECT_NE(function->tryGetStructuredCastTarget()->getType().get(), cloned_function->tryGetStructuredCastTarget()->getType().get());
    EXPECT_EQ(ast->getTreeHash(false), cloned->getTreeHash(false));
    EXPECT_EQ(formatted, cloned->formatWithSecretsOneLine());
    EXPECT_EQ(formatted, cloned->getColumnName());

    const auto reparsed = parseExpression(formatted);
    const auto * reparsed_function = getCastFunction(reparsed);
    ASSERT_NE(reparsed_function, nullptr);
    ASSERT_NE(reparsed_function->tryGetStructuredCastTarget(), nullptr);
    EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false));
    EXPECT_EQ(formatted, reparsed->formatWithSecretsOneLine());
}

TEST(UDTCastTarget, WrapperOwnsTheWholeNestedTarget)
{
    const auto ast = parseExpression("CAST(1 AS Array(app.UserId))");
    const auto * function = getCastFunction(ast);
    ASSERT_NE(function, nullptr);

    const auto * target = function->tryGetStructuredCastTarget();
    ASSERT_NE(target, nullptr);
    EXPECT_EQ(target, function->arguments->children[1]->as<ASTCastTarget>());
    EXPECT_EQ(target->children.size(), 1);
    EXPECT_EQ(target->children[0], target->getType());
    EXPECT_EQ(target->getType()->as<ASTUDTReference>(), nullptr);

    const auto * array = target->getType()->as<ASTDataType>();
    ASSERT_NE(array, nullptr);
    const auto arguments = array->getArguments();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 1);
    EXPECT_NE(arguments->children[0]->as<ASTUDTReference>(), nullptr);
}
