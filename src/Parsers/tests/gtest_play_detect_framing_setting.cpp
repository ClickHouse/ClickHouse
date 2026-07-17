#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

/** Regression coverage for the `detectFramingSetting` logic in `programs/server/play.html`.
  *
  * The Web UI decides whether a query carries its own `framing_output_format` setting - and, if so,
  * whether that setting disables framing (`= 'None'`) - to leave a query that chose its own framing
  * untouched and to refuse a query that turns framing off (the page renders only the framed path).
  *
  * The detection tokenizes the query with the ClickHouse `Lexer` (compiled to WebAssembly from
  * `src/Parsers/Lexer.cpp` - the very same source exercised here) and counts
  * `framing_output_format = value` only when it appears in a real settings context: a top-level
  * `SETTINGS` clause (at bracket depth 0) or a standalone `SET` statement (the setting is the leading
  * keyword). This ignores a `framing_output_format` mention inside a string literal or a comment, e.g.
  * `SELECT 'framing_output_format = None'` (wrongly refused) or `SELECT 'framing_output_format'`
  * (framing wrongly dropped), and - crucially - an ordinary comparison in the query body, e.g.
  * `SELECT framing_output_format = 'None' FROM values('framing_output_format String', ('None'))`,
  * which never sets the query setting. The `SETTINGS` clause may follow the `FORMAT` clause.
  *
  * There is no JavaScript/WebAssembly runtime in CI, so we cannot run the browser code directly.
  * Instead we reproduce the token-walking algorithm here on top of the real `DB::Lexer`. The lexer
  * (the part most likely to evolve) is shared; only the small detection below is a port. Keep this
  * in sync with `detectFramingSetting` in `programs/server/play.html`.
  */

namespace
{

std::string toLower(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

/// Mirror of `tokenize` in play.html, keeping only significant tokens (the browser filters
/// `.filter(t => t.significant)`): for each we record the token type and its text.
struct Tok
{
    DB::TokenType type;
    std::string text;
};

std::vector<Tok> tokenizeSignificant(const std::string & query)
{
    DB::Lexer lexer(query.data(), query.data() + query.size(), 65536);
    std::vector<Tok> tokens;
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError() || token.isEnd())
            break;
        if (token.isSignificant())
            tokens.push_back({token.type, std::string(token.begin, token.end)});
    }
    return tokens;
}

/// Mirror of `OPENING_BRACKETS` / `CLOSING_BRACKETS` in play.html.
bool isOpeningBracket(DB::TokenType type)
{
    return type == DB::TokenType::OpeningRoundBracket
        || type == DB::TokenType::OpeningSquareBracket
        || type == DB::TokenType::OpeningCurlyBrace;
}

bool isClosingBracket(DB::TokenType type)
{
    return type == DB::TokenType::ClosingRoundBracket
        || type == DB::TokenType::ClosingSquareBracket
        || type == DB::TokenType::ClosingCurlyBrace;
}

struct FramingSetting
{
    bool user_framing;
    bool user_disables_framing;
};

/// Faithful port of `detectFramingSetting` from play.html.
FramingSetting detectFramingSetting(const std::string & query)
{
    const std::vector<Tok> tokens = tokenizeSignificant(query);
    int depth = 0;
    bool in_settings = false;
    /// True at the start of the query and right after a top-level `;`, so a leading `SET` is
    /// recognized per statement.
    bool at_statement_start = true;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const Tok & t = tokens[i];
        const bool is_top_level_semicolon = depth == 0 && t.type == DB::TokenType::Semicolon;
        if (isOpeningBracket(t.type))
        {
            ++depth;
        }
        else if (isClosingBracket(t.type))
        {
            if (depth > 0)
                --depth;
        }
        else if (is_top_level_semicolon)
        {
            in_settings = false;
        }
        else if (t.type == DB::TokenType::BareWord && depth == 0)
        {
            const std::string lower = toLower(t.text);
            if (lower == "settings" || (lower == "set" && at_statement_start))
            {
                in_settings = true;
            }
            else if (in_settings
                && lower == "framing_output_format"
                && i + 1 < tokens.size() && tokens[i + 1].type == DB::TokenType::Equals)
            {
                /// The value is the next significant token; a string literal carries its surrounding quotes.
                std::string value = (i + 2 < tokens.size()) ? tokens[i + 2].text : "";
                if (i + 2 < tokens.size() && tokens[i + 2].type == DB::TokenType::StringLiteral && value.size() >= 2)
                    value = value.substr(1, value.size() - 2);
                if (toLower(value) == "none")
                    return {false, true};
                return {true, false};
            }
        }
        /// The next token starts a new statement only right after a top-level `;`.
        at_statement_start = is_top_level_semicolon;
    }
    return {false, false};
}

void expectFraming(const std::string & query, bool user_framing, bool user_disables_framing)
{
    const FramingSetting result = detectFramingSetting(query);
    EXPECT_EQ(result.user_framing, user_framing) << "query: " << query;
    EXPECT_EQ(result.user_disables_framing, user_disables_framing) << "query: " << query;
}

}

TEST(PlayDetectFramingSetting, NoSetting)
{
    expectFraming("SELECT 1", false, false);
    expectFraming("SELECT 1 SETTINGS max_threads = 4", false, false);
    expectFraming("", false, false);
}

TEST(PlayDetectFramingSetting, RealSettingEnablesUserFraming)
{
    expectFraming("SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString'", true, false);
    expectFraming("SELECT 1 SETTINGS framing_output_format='EventStream'", true, false);
    /// Case-insensitive setting name and other settings around it.
    expectFraming("SELECT 1 SETTINGS max_threads = 2, FRAMING_OUTPUT_FORMAT = 'EventStream', max_block_size = 1", true, false);
    /// A standalone `SET` statement is a real assignment too.
    expectFraming("SET framing_output_format = 'JSONEachPacketString'", true, false);
    /// An unquoted value is still a real assignment.
    expectFraming("SELECT 1 SETTINGS framing_output_format = EventStream", true, false);
    /// The `SETTINGS` clause may follow the `FORMAT` clause.
    expectFraming("SELECT 1 FORMAT TSV SETTINGS framing_output_format = 'EventStream'", true, false);
}

TEST(PlayDetectFramingSetting, NoneDisablesFraming)
{
    expectFraming("SELECT 1 SETTINGS framing_output_format = 'None'", false, true);
    expectFraming("select 1 settings framing_output_format='none'", false, true);
    /// Unquoted `None`.
    expectFraming("SELECT 1 SETTINGS framing_output_format = None", false, true);
    /// `SETTINGS` after `FORMAT` disables framing too.
    expectFraming("SELECT 1 FORMAT TSV SETTINGS framing_output_format = 'None'", false, true);
}

TEST(PlayDetectFramingSetting, StringLiteralIsNotASetting)
{
    /// The reported bug: a `framing_output_format` mention inside a string literal must not be
    /// treated as a real setting - neither the enable nor the disable path may trigger.
    expectFraming("SELECT 'framing_output_format = None'", false, false);
    expectFraming("SELECT 'framing_output_format'", false, false);
    expectFraming("SELECT 'framing_output_format = EventStream' AS x", false, false);
}

TEST(PlayDetectFramingSetting, ComparisonInBodyIsNotASetting)
{
    /// The reported bug: `framing_output_format = <value>` used as an ordinary comparison in the
    /// query body (no `SETTINGS`/`SET` in front) never sets the query setting, so the page must
    /// neither refuse it nor drop its own framing.
    expectFraming("SELECT framing_output_format = 'None' FROM values('framing_output_format String', ('None'))", false, false);
    expectFraming("SELECT framing_output_format = 'EventStream'", false, false);
    /// A comparison before a real `SETTINGS` clause is still ignored; the real setting wins.
    expectFraming("SELECT framing_output_format = 1 SETTINGS framing_output_format = 'EventStream'", true, false);
}

TEST(PlayDetectFramingSetting, CommentIsNotASetting)
{
    expectFraming("SELECT 1 -- framing_output_format = None\n", false, false);
    expectFraming("SELECT 1 /* framing_output_format = 'EventStream' */", false, false);
}

TEST(PlayDetectFramingSetting, RealSettingWinsOverStringMention)
{
    /// A real setting alongside a string-literal mention is still detected.
    expectFraming("SELECT 'framing_output_format' SETTINGS framing_output_format = 'None'", false, true);
    expectFraming("SELECT 'framing_output_format = None' SETTINGS framing_output_format = 'EventStream'", true, false);
}
