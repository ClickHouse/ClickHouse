#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParser.h>
#include <Parsers/LiteralTokenInfo.h>
#include <Parsers/TokenIterator.h>

#include <gtest/gtest.h>

#include <random>
#include <unordered_map>
#include <vector>

using namespace DB;

namespace
{

/// The map is keyed by `ASTLiteral` addresses but never dereferences them, so the tests can use
/// made-up aligned addresses rather than build real literals.
const ASTLiteral * fakeLiteral(uintptr_t n)
{
    return reinterpret_cast<const ASTLiteral *>((n + 1) * alignof(void *));
}

LiteralTokenInfo someTokenInfo(uintptr_t n)
{
    return LiteralTokenInfo{reinterpret_cast<const char *>(n * 2 + 1), reinterpret_cast<const char *>(n * 2 + 2)};
}

}

TEST(LiteralTokenMap, EmptyMapFindsNothing)
{
    LiteralTokenMap map;
    EXPECT_EQ(map.find(fakeLiteral(0)), nullptr);
}

TEST(LiteralTokenMap, FindsWhatWasInserted)
{
    LiteralTokenMap map;
    map.insert_or_assign(fakeLiteral(1), someTokenInfo(1));

    const auto * found = map.find(fakeLiteral(1));
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->begin, someTokenInfo(1).begin);
    EXPECT_EQ(found->end, someTokenInfo(1).end);

    EXPECT_EQ(map.find(fakeLiteral(2)), nullptr);
}

TEST(LiteralTokenMap, InsertOverwrites)
{
    /// Nested literals can reuse the address of a discarded node, so the last write must win.
    LiteralTokenMap map;
    map.insert_or_assign(fakeLiteral(7), someTokenInfo(1));
    map.insert_or_assign(fakeLiteral(7), someTokenInfo(2));

    const auto * found = map.find(fakeLiteral(7));
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->begin, someTokenInfo(2).begin);
    EXPECT_EQ(found->end, someTokenInfo(2).end);
}

TEST(LiteralTokenMap, ForgetHidesAnEntry)
{
    /// A parser that discards a subtree forgets the literals in it, so that a literal created at a
    /// reused address does not inherit their token positions.
    LiteralTokenMap map;
    map.insert_or_assign(fakeLiteral(3), someTokenInfo(1));
    map.forget(fakeLiteral(3));
    EXPECT_EQ(map.find(fakeLiteral(3)), nullptr);

    /// Forgetting a literal that was never recorded is allowed, and neighbours are unaffected.
    map.insert_or_assign(fakeLiteral(4), someTokenInfo(2));
    map.forget(fakeLiteral(5));
    EXPECT_EQ(map.find(fakeLiteral(5)), nullptr);
    ASSERT_NE(map.find(fakeLiteral(4)), nullptr);
    EXPECT_EQ(map.find(fakeLiteral(4))->begin, someTokenInfo(2).begin);

    /// And a forgotten address can be recorded again.
    map.insert_or_assign(fakeLiteral(3), someTokenInfo(9));
    ASSERT_NE(map.find(fakeLiteral(3)), nullptr);
    EXPECT_EQ(map.find(fakeLiteral(3))->begin, someTokenInfo(9).begin);
}

TEST(LiteralTokenMap, GrowsBeyondInlineCapacity)
{
    /// Well past the inline storage, so the table rehashes onto the heap more than once.
    constexpr size_t count = 1000;
    LiteralTokenMap map;
    for (size_t i = 0; i < count; ++i)
        map.insert_or_assign(fakeLiteral(i), someTokenInfo(i));

    for (size_t i = 0; i < count; ++i)
    {
        const auto * found = map.find(fakeLiteral(i));
        ASSERT_NE(found, nullptr) << "missing entry " << i;
        EXPECT_EQ(found->begin, someTokenInfo(i).begin);
    }
    EXPECT_EQ(map.find(fakeLiteral(count)), nullptr);
}

TEST(LiteralTokenMap, AgreesWithUnorderedMap)
{
    std::mt19937_64 rng(12345); /// NOLINT(cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible

    for (int round = 0; round < 500; ++round)
    {
        LiteralTokenMap map;
        std::unordered_map<const ASTLiteral *, LiteralTokenInfo> reference;
        std::vector<const ASTLiteral *> keys;

        size_t inserts = rng() % 400;
        for (size_t i = 0; i < inserts; ++i)
        {
            const ASTLiteral * key = nullptr;
            if (!keys.empty() && rng() % 4 == 0)
                key = keys[rng() % keys.size()];    /// exercise overwriting
            else
            {
                key = fakeLiteral(rng() % 100000);
                keys.push_back(key);
            }

            auto value = someTokenInfo(rng());
            map.insert_or_assign(key, value);
            reference.insert_or_assign(key, value);
        }

        for (const auto * key : keys)
        {
            const auto * found = map.find(key);
            auto it = reference.find(key);
            ASSERT_NE(found, nullptr);
            ASSERT_NE(it, reference.end());
            EXPECT_EQ(found->begin, it->second.begin);
            EXPECT_EQ(found->end, it->second.end);
        }

        for (int i = 0; i < 50; ++i)
        {
            const auto * absent = fakeLiteral(100000 + rng() % 100000);
            if (!reference.contains(absent))
                EXPECT_EQ(map.find(absent), nullptr);
        }
    }
}


/// The tests below cover the `has_token_info` bit rather than the map mechanics above.
/// The invariant: a literal reporting `hasTokenInfo` has a valid span in the map, and a
/// parser-synthesized literal reports false. `forget` covers subtrees the parser discards;
/// the bit covers synthesized literals it keeps, which have no subtree to forget.

namespace
{

/// Reads the token-info marker where it exists, and falls back to the map lookup that stands in for
/// it without the fix, so this file also compiles against the merge base. The template keeps the
/// discarded branch uninstantiated.
template <typename Literal>
bool literalHasTokenInfo(const Literal & literal, const LiteralTokenMap & token_map)
{
    if constexpr (requires { literal.hasTokenInfo(); })
        return literal.hasTokenInfo();
    else
        return token_map.find(&literal) != nullptr;
}

/// Leave a dead span in the map at `literal`'s own address, the state an allocator produces when it
/// hands that address to a new literal. Staged rather than awaited, so the check is deterministic.
void recordStaleSpanAt(LiteralTokenMap & token_map, const ASTLiteral * literal)
{
    token_map.insert_or_assign(literal, someTokenInfo(999));
}

void collectLiterals(const ASTPtr & ast, std::vector<const ASTLiteral *> & out)
{
    if (const auto * literal = ast->as<ASTLiteral>())
        out.push_back(literal);
    for (const auto & child : ast->children)
        collectLiterals(child, out);
}

ASTPtr parseWithTokenMap(const std::string & expr, LiteralTokenMap & token_map)
{
    Tokens tokens(expr.data(), expr.data() + expr.size());
    IParser::Pos pos(tokens, 1000, 1000);
    Expected expected;
    expected.literal_token_map = &token_map;
    ParserExpression parser;
    ASTPtr ast;
    EXPECT_TRUE(parser.parse(pos, ast, expected)) << "failed to parse: " << expr;
    return ast;
}

/// A tokenized literal must be in the map; an untokenized literal must not report token info.
void checkInvariant(const ASTPtr & ast, const LiteralTokenMap & token_map)
{
    std::vector<const ASTLiteral *> literals;
    collectLiterals(ast, literals);
    for (const auto * literal : literals)
    {
        if (literalHasTokenInfo(*literal, token_map))
        {
            EXPECT_NE(token_map.find(literal), nullptr)
                << "literal flagged `hasTokenInfo` must have a token map entry: " << literal->getID(' ');
        }
    }
}

/// Return literals that were NOT tokenized from the query (synthesized by a parser).
std::vector<const ASTLiteral *> synthesizedLiterals(const ASTPtr & ast, const LiteralTokenMap & token_map)
{
    std::vector<const ASTLiteral *> all;
    collectLiterals(ast, all);
    std::vector<const ASTLiteral *> out;
    for (const auto * literal : all)
        if (!literalHasTokenInfo(*literal, token_map))
            out.push_back(literal);
    return out;
}

}

/// `ParserCastOperator` synthesizes the cast value, so it can inherit the span of the
/// discarded `'sumState'` string-literal probe.
TEST(LiteralTokenMap, NumericCastOperatorLiteralHasNoTokenInfo)
{
    LiteralTokenMap token_map;
    ASTPtr ast = parseWithTokenMap("initializeAggregation('sumState', 0::UInt64)", token_map);
    checkInvariant(ast, token_map);

    std::vector<const ASTLiteral *> literals;
    collectLiterals(ast, literals);
    bool saw_cast_value = false;
    for (const auto * literal : literals)
    {
        /// The cast value `'0'` survives as `ASTLiteral(String "0")`; it must not report token info.
        if (literal->value.getType() == Field::Types::String && literal->value.safeGet<String>() == "0")
        {
            saw_cast_value = true;
            recordStaleSpanAt(token_map, literal);
            EXPECT_FALSE(literalHasTokenInfo(*literal, token_map))
                << "synthesized numeric-cast literal '0' must not report token info";
        }
    }
    EXPECT_TRUE(saw_cast_value) << "expected the synthesized numeric-cast literal '0' in the AST";
}

/// The literal synthesized by `ParserMySQLGlobalVariable` (`@@name` -> `globalVariable('name')`) survives
/// in a templatable position but is never tokenized, so it must not report token info.
TEST(LiteralTokenMap, GlobalVariableLiteralHasNoTokenInfo)
{
    LiteralTokenMap token_map;
    ASTPtr ast = parseWithTokenMap("@@max_allowed_packet", token_map);
    checkInvariant(ast, token_map);

    std::vector<const ASTLiteral *> literals;
    collectLiterals(ast, literals);
    ASSERT_FALSE(literals.empty()) << "expected the globalVariable name literal in the AST";
    for (const auto * literal : literals)
        recordStaleSpanAt(token_map, literal);

    EXPECT_EQ(synthesizedLiterals(ast, token_map).size(), literals.size())
        << "globalVariable name literal must be synthesized (no token info)";
}

/// Slow-path Tuple literal: `((1, 2))` produces an `ASTLiteral(Tuple)` synthesized in `getResultImpl`,
/// without recording token info. It must not report token info.
TEST(LiteralTokenMap, SlowPathTupleLiteralHasNoTokenInfo)
{
    LiteralTokenMap token_map;
    ASTPtr ast = parseWithTokenMap("materialize(1) IN ((1, 2))", token_map);
    checkInvariant(ast, token_map);

    std::vector<const ASTLiteral *> literals;
    collectLiterals(ast, literals);
    bool saw_tuple = false;
    for (const auto * literal : literals)
    {
        if (literal->value.getType() == Field::Types::Tuple)
        {
            saw_tuple = true;
            recordStaleSpanAt(token_map, literal);
            EXPECT_FALSE(literalHasTokenInfo(*literal, token_map))
                << "synthesized slow-path Tuple literal must not report token info";
        }
    }
    EXPECT_TRUE(saw_tuple) << "expected a synthesized Tuple literal in the AST";
}

/// An array of literals is consumed by the fast path as ONE composite literal, which the parser
/// tokenizes, so it must report token info and be findable. This is the positive counterpart of the
/// Tuple case above: the same `Field::Types` check there sees a synthesized literal, here a recorded
/// one, so the bit tracks how the literal was built rather than what type it holds.
TEST(LiteralTokenMap, ArrayLiteralTokenInfoInvariant)
{
    LiteralTokenMap token_map;
    ASTPtr ast = parseWithTokenMap("x = [1, 2, 3]", token_map);
    checkInvariant(ast, token_map);

    std::vector<const ASTLiteral *> literals;
    collectLiterals(ast, literals);
    bool saw_array = false;
    for (const auto * literal : literals)
    {
        if (literal->value.getType() == Field::Types::Array)
        {
            saw_array = true;
            EXPECT_TRUE(literalHasTokenInfo(*literal, token_map)) << "fast-path Array literal is tokenized";
            EXPECT_NE(token_map.find(literal), nullptr);
        }
    }
    EXPECT_TRUE(saw_array) << "expected an Array literal in the AST";
}

/// `EXTRACT` injects `UInt64` literals into `plus`/`minus`/`intDiv` chains (`buildExtractTimePartAST`) that are
/// never tokenized; those must not report token info.
TEST(LiteralTokenMap, ExtractInjectedLiteralsHaveNoTokenInfo)
{
    LiteralTokenMap token_map;
    ASTPtr ast = parseWithTokenMap("EXTRACT(CENTURY FROM materialize(now()))", token_map);
    checkInvariant(ast, token_map);

    /// The 1, 100, 1 injected constants are all synthesized.
    std::vector<const ASTLiteral *> injected = synthesizedLiterals(ast, token_map);
    ASSERT_FALSE(injected.empty()) << "EXTRACT(CENTURY ...) must inject synthesized numeric literals";
    for (const auto * literal : injected)
    {
        recordStaleSpanAt(token_map, literal);
        EXPECT_FALSE(literalHasTokenInfo(*literal, token_map))
            << "EXTRACT-injected literal must not report token info";
    }
}

/// Covers all three copy paths: `clone`, copy construction and assignment, plus self-assignment.
TEST(LiteralTokenMap, CloneDoesNotInheritTokenInfo)
{
    LiteralTokenMap token_map;
    ASTPtr ast = parseWithTokenMap("x = 12345", token_map);

    std::vector<const ASTLiteral *> literals;
    collectLiterals(ast, literals);
    bool saw = false;
    for (const auto * literal : literals)
    {
        if (!literalHasTokenInfo(*literal, token_map))
            continue;
        saw = true;

        /// Each copy below gets the hazard staged at its own address, so the map holds a dead span
        /// for it exactly as an allocator handing out a freed literal's address would.
        ASTPtr copy = literal->clone();
        const auto * cloned = copy->as<ASTLiteral>();
        ASSERT_NE(cloned, nullptr);
        recordStaleSpanAt(token_map, cloned);
        EXPECT_FALSE(literalHasTokenInfo(*cloned, token_map)) << "a clone owns no map entry, so it must not claim one";

        /// Not only through `clone`: callers copy-construct literals directly (for instance
        /// `ConstantNode::toAST` and `RewriteSumFunctionWithSumAndCountVisitor`), so the copy
        /// constructor itself has to clear it.
        ASTLiteral direct_copy(*literal);
        recordStaleSpanAt(token_map, &direct_copy);
        EXPECT_FALSE(literalHasTokenInfo(direct_copy, token_map)) << "a copy-constructed literal owns no map entry";

        /// Assignment lands in an object that already exists at its own address, so it has the
        /// same problem as copy construction.
        ASTLiteral assigned(Field{UInt64{0}});
        assigned = *literal;
        recordStaleSpanAt(token_map, &assigned);
        EXPECT_FALSE(literalHasTokenInfo(assigned, token_map)) << "an assigned-to literal owns no map entry";

        /// Self-assignment must not throw away token info the literal legitimately owns. Routed
        /// through a reference so the compiler does not see a literal `x = x` and reject it.
        ASTLiteral & self = const_cast<ASTLiteral &>(*literal);
        ASTLiteral & alias_of_self = self;
        self = alias_of_self;
        EXPECT_TRUE(literalHasTokenInfo(*literal, token_map)) << "self-assignment must keep its own token info";
        EXPECT_NE(token_map.find(literal), nullptr);
    }
    EXPECT_TRUE(saw) << "expected at least one recorded literal to clone";
}

/// Sanity: a real tokenized literal DOES report token info and is present in the map.
TEST(LiteralTokenMap, TokenizedLiteralReportsTokenInfo)
{
    LiteralTokenMap token_map;
    ASTPtr ast = parseWithTokenMap("x = 12345", token_map);
    checkInvariant(ast, token_map);

    std::vector<const ASTLiteral *> literals;
    collectLiterals(ast, literals);
    bool saw = false;
    for (const auto * literal : literals)
    {
        if (literal->value.getType() == Field::Types::UInt64 && literal->value.safeGet<UInt64>() == 12345)
        {
            saw = true;
            EXPECT_TRUE(literalHasTokenInfo(*literal, token_map)) << "tokenized numeric literal must report token info";
            EXPECT_NE(token_map.find(literal), nullptr);
        }
    }
    EXPECT_TRUE(saw);
}
