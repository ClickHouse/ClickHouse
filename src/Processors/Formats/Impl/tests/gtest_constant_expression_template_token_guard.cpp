#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/LiteralTokenInfo.h>
#include <Parsers/TokenIterator.h>
#include <Processors/Formats/Impl/ConstantExpressionTemplate.h>

#include <gtest/gtest.h>

#include <functional>
#include <stdexcept>
#include <vector>

using namespace DB;

namespace
{

void collectLiterals(const ASTPtr & ast, std::vector<const ASTLiteral *> & out)
{
    if (const auto * literal = ast->as<ASTLiteral>())
        out.push_back(literal);
    for (const auto & child : ast->children)
        collectLiterals(child, out);
}

/// Deduce a template the way `ValuesBlockInputFormat` does and render it. `stage_stale` runs between
/// parsing and deduction, so a test can put the map into the state address reuse produces.
String deduceTemplate(
    const std::string & expr,
    const String & result_type_name,
    const std::function<void(const ASTPtr &, LiteralTokenMap &)> & stage_stale = {})
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    Tokens tokens(expr.data(), expr.data() + expr.size());
    IParser::Pos pos(tokens, 1000, 1000);
    TokenIterator begin(tokens);

    LiteralTokenMap token_map;
    Expected expected;
    expected.literal_token_map = &token_map;

    ASTPtr ast;
    ParserExpression parser;
    if (!parser.parse(pos, ast, expected))
        throw std::runtime_error("failed to parse: " + expr);

    if (stage_stale)
        stage_stale(ast, token_map);

    ConstantExpressionTemplate::Cache cache;
    auto structure = cache.getFromCacheOrConstruct(
        DataTypeFactory::instance().get(result_type_name),
        /*null_as_default=*/false,
        begin,
        /*expression_end=*/pos,
        ast,
        token_map,
        getContext().context,
        /*found_in_cache=*/nullptr,
        /*salt=*/")");

    return structure->dumpTemplate();
}

}

/// A literal the parser synthesized must not be turned into a template placeholder, however the
/// token map is keyed. `ParserCastOperator` keeps the type as text and builds `'1.5'` for the value
/// after discarding the AST of `Decimal32(3)`, so the allocator can hand `'1.5'` the address the
/// type argument `3` used - and with it that argument's recorded span.
TEST(ConstantExpressionTemplateTokenGuard, SynthesizedCastLiteralIsNotTemplated)
{
    const std::string expr = "1.5::Decimal32(3)";
    const size_t type_argument_offset = expr.find("(3)") + 1;

    auto stage_type_argument_span = [&](const ASTPtr & ast, LiteralTokenMap & token_map)
    {
        std::vector<const ASTLiteral *> literals;
        collectLiterals(ast, literals);

        const ASTLiteral * cast_value = nullptr;
        for (const auto * literal : literals)
            if (literal->value.getType() == Field::Types::String && literal->value.safeGet<String>() == "1.5")
                cast_value = literal;
        ASSERT_NE(cast_value, nullptr) << "expected the synthesized cast value literal '1.5'";

        const char * stale_begin = expr.data() + type_argument_offset;
        token_map.insert_or_assign(cast_value, LiteralTokenInfo{stale_begin, stale_begin + 1});
    };

    /// Every token stays a token: the type argument keeps its own `'3'` and no placeholder appears.
    EXPECT_EQ(
        deduceTemplate(expr, "Decimal64(3)", stage_type_argument_span),
        "'1.5', '::', 'Decimal32', '(', '3', ')', eof");
}

/// Control for the test above: a literal that really was tokenized still becomes a placeholder, so
/// an absent placeholder there is a fact about the guard and not about this file's rendering.
TEST(ConstantExpressionTemplateTokenGuard, TokenizedLiteralIsTemplated)
{
    EXPECT_EQ(deduceTemplate("1 + 2", "UInt64"), "UInt64, '+', UInt64, eof");
}
