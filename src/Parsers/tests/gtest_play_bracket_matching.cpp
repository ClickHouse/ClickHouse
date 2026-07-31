#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <string>
#include <vector>

/** Regression coverage for the `computeBracketInfo` logic in `programs/server/play.html`.
  *
  * `play.html` colors brackets (rainbow parentheses) and highlights the matched pair around
  * the cursor by tokenizing the textarea content with the ClickHouse `Lexer` (compiled to
  * WebAssembly from `src/Parsers/Lexer.cpp` — the very same source exercised here) and then
  * walking the tokens with a type-aware stack.
  *
  * There is no JavaScript/WebAssembly runtime in CI, so we cannot run the browser code
  * directly. Instead we reproduce the exact stack algorithm here on top of the real
  * `DB::Lexer`. The lexer (the part most likely to evolve) is shared; only the small
  * depth-assignment algorithm below is a port. Keep this in sync with `computeBracketInfo`
  * in `programs/server/play.html`.
  *
  * The contract being locked: a bracket is colored (given a rainbow depth) only once it has
  * a real, properly-nested mate. A lone `(`, a stray `)`, or a mismatched pair stays at
  * depth -1. Once nesting is broken (a closer that does not match the innermost opener), the
  * outstanding openers are discarded so a later closer cannot reach over the break and
  * retroactively match an opener — e.g. `([)]` colors nothing. A `;` is not a boundary: like
  * `clickhouse-client`, brackets still match across it (`SELECT (; SELECT )` pairs the `(`
  * with the `)`).
  */

namespace
{

using DB::TokenType;

struct Tok
{
    TokenType type;
};

std::vector<Tok> tokenize(const std::string & query)
{
    DB::Lexer lexer(query.data(), query.data() + query.size(), 65536);
    std::vector<Tok> tokens;
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError() || token.isEnd())
            break;
        tokens.push_back({token.type});
    }
    return tokens;
}

bool isOpening(TokenType type)
{
    return type == TokenType::OpeningRoundBracket || type == TokenType::OpeningSquareBracket
        || type == TokenType::OpeningCurlyBrace;
}

bool isClosing(TokenType type)
{
    return type == TokenType::ClosingRoundBracket || type == TokenType::ClosingSquareBracket
        || type == TokenType::ClosingCurlyBrace;
}

/// The closing type that matches each opening type (Whitespace as a "never a bracket" sentinel).
TokenType matchingClose(TokenType opening)
{
    switch (opening)
    {
        case TokenType::OpeningRoundBracket:  return TokenType::ClosingRoundBracket;
        case TokenType::OpeningSquareBracket: return TokenType::ClosingSquareBracket;
        case TokenType::OpeningCurlyBrace:    return TokenType::ClosingCurlyBrace;
        default:                              return TokenType::Whitespace;
    }
}

/// Faithful port of `computeBracketInfo` (the depth + match arrays) from play.html.
struct BracketInfo
{
    std::vector<ssize_t> depth;
    std::vector<ssize_t> match_of;
};

BracketInfo computeBracketInfo(const std::vector<Tok> & tokens)
{
    BracketInfo info;
    info.depth.assign(tokens.size(), -1);
    info.match_of.assign(tokens.size(), -1);

    std::vector<size_t> stack;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const TokenType type = tokens[i].type;
        if (isOpening(type))
        {
            stack.push_back(i);
        }
        else if (isClosing(type))
        {
            if (!stack.empty() && matchingClose(tokens[stack.back()].type) == type)
            {
                const size_t top = stack.back();
                stack.pop_back();
                info.match_of[i] = static_cast<ssize_t>(top);
                info.match_of[top] = static_cast<ssize_t>(i);
                info.depth[top] = info.depth[i] = static_cast<ssize_t>(stack.size());
            }
            else if (!stack.empty())
            {
                /// Mismatched closer breaks the nesting: discard every outstanding opener.
                stack.clear();
            }
            /// A closing bracket with no match keeps depth -1.
        }
    }
    return info;
}

/// The rainbow depth of each bracket token (opening or closing), in source order.
std::vector<ssize_t> bracketDepths(const std::string & query)
{
    const std::vector<Tok> tokens = tokenize(query);
    const BracketInfo info = computeBracketInfo(tokens);
    std::vector<ssize_t> result;
    for (size_t i = 0; i < tokens.size(); ++i)
        if (isOpening(tokens[i].type) || isClosing(tokens[i].type))
            result.push_back(info.depth[i]);
    return result;
}

}

TEST(PlayBracketMatching, ProperNesting)
{
    EXPECT_EQ(bracketDepths("()"), (std::vector<ssize_t>{0, 0}));
    EXPECT_EQ(bracketDepths("(())"), (std::vector<ssize_t>{0, 1, 1, 0}));
    EXPECT_EQ(bracketDepths("((()))"), (std::vector<ssize_t>{0, 1, 2, 2, 1, 0}));
    EXPECT_EQ(bracketDepths("()()"), (std::vector<ssize_t>{0, 0, 0, 0}));
    /// Mixed bracket types nest and match like the round-only client, but across all kinds.
    EXPECT_EQ(bracketDepths("([{}])"), (std::vector<ssize_t>{0, 1, 2, 2, 1, 0}));
    EXPECT_EQ(bracketDepths("SELECT arr[1] FROM (SELECT [1,2] AS arr)"),
              (std::vector<ssize_t>{0, 0, 0, 1, 1, 0}));
}

TEST(PlayBracketMatching, UnmatchedBracketsStayDefault)
{
    /// A lone opener, a stray closer, or a simple mismatch is never colored.
    EXPECT_EQ(bracketDepths("("), (std::vector<ssize_t>{-1}));
    EXPECT_EQ(bracketDepths(")"), (std::vector<ssize_t>{-1}));
    EXPECT_EQ(bracketDepths(")("), (std::vector<ssize_t>{-1, -1}));
    EXPECT_EQ(bracketDepths("( ]"), (std::vector<ssize_t>{-1, -1}));
}

TEST(PlayBracketMatching, CrossingNestingColorsNothing)
{
    /// The AI-review repro: the `)` breaks the nesting, so the later `]` must NOT reach over
    /// it to match the `[`. Nothing gets a rainbow depth.
    EXPECT_EQ(bracketDepths("([)]"), (std::vector<ssize_t>{-1, -1, -1, -1}));
    EXPECT_EQ(bracketDepths("SELECT ([)]"), (std::vector<ssize_t>{-1, -1, -1, -1}));
    EXPECT_EQ(bracketDepths("{(}"), (std::vector<ssize_t>{-1, -1, -1}));
    /// A valid pair after the broken region is still colored.
    EXPECT_EQ(bracketDepths("([) ()"), (std::vector<ssize_t>{-1, -1, -1, 0, 0}));
}

TEST(PlayBracketMatching, MatchesAcrossSemicolonLikeClient)
{
    /// `clickhouse-client`'s rainbow loop skips `;` without clearing its bracket stack, so a
    /// `(` still pairs with a `)` in a later statement. The Web UI mirrors that (parity) rather
    /// than resetting the stack at `;`.
    EXPECT_EQ(bracketDepths("(; )"), (std::vector<ssize_t>{0, 0}));
    EXPECT_EQ(bracketDepths("SELECT (; SELECT )"), (std::vector<ssize_t>{0, 0}));
    /// A pair entirely within one statement, and a fresh pair after the `;`, also color.
    EXPECT_EQ(bracketDepths("(); ()"), (std::vector<ssize_t>{0, 0, 0, 0}));
    EXPECT_EQ(bracketDepths("SELECT (1); SELECT (2)"), (std::vector<ssize_t>{0, 0, 0, 0}));
}
