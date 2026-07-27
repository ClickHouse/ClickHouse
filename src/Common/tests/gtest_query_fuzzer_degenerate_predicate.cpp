#include <gtest/gtest.h>

#include <Common/QueryFuzzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>

using namespace DB;

namespace
{
/// `and()` is a parseable AST -- `Function and (children 1) / ExpressionList` with zero children.
ASTPtr parseExpression(const String & sql)
{
    ParserExpression parser;
    return parseQuery(parser, sql, 0, 0, 0);
}

bool isLogicalFunctionName(const String & name)
{
    return name == "and" || name == "or" || name == "xor";
}

/// A large negation probability keeps `tryNegateNextPredicate` on its identity path in almost every
/// run; the assertions below accept both outcomes, so they do not depend on the draw.
constexpr int NEG_PROB = 1000;
}

/// `extractPredicates` flattens an AND/OR/XOR tree into a list of operands. A degenerate call such
/// as `and()` has no operands, and flattening it used to append nothing, leaving the list empty and
/// tripping `chassert(!predicates.empty())` in `permutePredicateClause`. It must instead contribute
/// exactly one opaque leaf -- the node itself, not a copy.
TEST(QueryFuzzer, ExtractPredicatesKeepsDegenerateLeaf)
{
    ASTPtr node = parseExpression("and()");
    ASSERT_NE(nullptr, node);
    const auto * func = node->as<ASTFunction>();
    ASSERT_NE(nullptr, func);
    ASSERT_EQ(func->name, "and");
    ASSERT_TRUE(!func->arguments || func->arguments->children.empty());

    QueryFuzzer fuzzer{pcg64(1)};
    ASTs predicates;
    fuzzer.extractPredicates(node, predicates, "and", NEG_PROB);

    ASSERT_EQ(predicates.size(), 1u);
    EXPECT_EQ(predicates[0].get(), node.get());
}

/// The old guard was `func->name == op && func->arguments`, so a logical function with a null
/// argument list fell through to the branch that calls `permutePredicateClause` on the very same
/// node -- an unbounded mutual recursion. The parser always attaches an `ExpressionList`, so this
/// shape is only reachable from the fuzzer's own function-name swap; assert it terminates.
TEST(QueryFuzzer, ExtractPredicatesDoesNotRecurseOnNullArguments)
{
    auto func = make_intrusive<ASTFunction>();
    func->name = "and";
    ASSERT_EQ(nullptr, func->arguments);
    ASTPtr node = func;

    QueryFuzzer fuzzer{pcg64(1)};
    ASTs predicates;
    fuzzer.extractPredicates(node, predicates, "and", NEG_PROB);

    ASSERT_EQ(predicates.size(), 1u);
    EXPECT_EQ(predicates[0].get(), node.get());
}

/// `permutePredicateClause` must leave a degenerate logical call alone. Without its early return it
/// re-enters the shuffle path and rebuilds the node with `makeASTFunction(func->name, predicates)`,
/// wrapping the degenerate node inside a fresh logical call of the same name -- `and()` becomes
/// `and(and())`, which grows one nesting level on every visit. The only permitted outcomes are the
/// original node itself or a `not(...)` around it.
TEST(QueryFuzzer, PermuteDoesNotRewrapDegeneratePredicate)
{
    ASTPtr node = parseExpression("and()");
    ASSERT_NE(nullptr, node);
    ASSERT_NE(nullptr, node->as<ASTFunction>());

    QueryFuzzer fuzzer{pcg64(1)};
    ASTPtr result = fuzzer.permutePredicateClause(node, NEG_PROB);

    ASSERT_NE(nullptr, result);
    const auto * result_func = result->as<ASTFunction>();
    ASSERT_NE(nullptr, result_func);

    if (isLogicalFunctionName(result_func->name))
    {
        /// Not permuted: the same node came back, still without operands.
        EXPECT_EQ(result.get(), node.get());
        EXPECT_TRUE(!result_func->arguments || result_func->arguments->children.empty());
    }
    else
    {
        /// Negated: `not` around the original node, nothing else.
        EXPECT_EQ(result_func->name, "not");
        ASSERT_NE(nullptr, result_func->arguments);
        ASSERT_EQ(result_func->arguments->children.size(), 1u);
        EXPECT_EQ(result_func->arguments->children[0].get(), node.get());
    }
}
