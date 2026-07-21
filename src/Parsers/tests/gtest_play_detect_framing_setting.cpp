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
  * keyword). A settings context is recognized by its list grammar, not by the keyword alone: the
  * keyword must be followed by `name = value` pairs separated by commas, and the context ends where
  * that list grammar ends, so a column merely named `settings` does not open one. This ignores a `framing_output_format` mention inside a string literal or a comment, e.g.
  * `SELECT 'framing_output_format = None'` (wrongly refused) or `SELECT 'framing_output_format'`
  * (framing wrongly dropped), and - crucially - an ordinary comparison in the query body, e.g.
  * `SELECT framing_output_format = 'None' FROM values('framing_output_format String', ('None'))`,
  * which never sets the query setting. The `SETTINGS` clause may follow the `FORMAT` clause.
  *
  * A standalone `SET framing_output_format = ...` is reported separately (`user_sets_session_framing`)
  * from a query-level `SETTINGS` clause: it changes the setting for the whole session (when the
  * connection uses a `session_id`), which the page cannot honor because it appends its own
  * `framing_output_format` to every request, so it is refused rather than silently overridden.
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
    bool user_sets_session_framing;
};

/// Faithful port of `detectFramingSetting` from play.html.
FramingSetting detectFramingSetting(const std::string & query)
{
    const std::vector<Tok> tokens = tokenizeSignificant(query);
    int depth = 0;
    /// True at the start of the query and right after a top-level `;`, so a leading `SET` is
    /// recognized per statement.
    bool at_statement_start = true;
    /// The server keeps duplicate `SETTINGS` entries and applies them in order, so the last
    /// assignment wins: `SETTINGS framing_output_format = 'None', framing_output_format =
    /// 'EventStream'` leaves framing enabled. The walk therefore records every assignment
    /// (each overwriting the previous) and classifies after it, instead of exiting on the first.
    bool saw_query_setting = false;
    bool query_setting_disables = false;
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
        else if (t.type == DB::TokenType::BareWord && depth == 0)
        {
            const std::string lower = toLower(t.text);
            const bool is_settings = lower == "settings";
            const bool is_set = lower == "set" && at_statement_start;
            /// A real settings context is not just the keyword: the keyword must be followed by an
            /// actual settings list, which always begins `name = value`. A column merely named
            /// `settings` in the query body is followed by a `,`/`FROM`/operator instead, so it
            /// does not open a settings context and a comparison next to it is not mistaken for a
            /// setting.
            if ((is_settings || is_set)
                && i + 2 < tokens.size()
                && tokens[i + 1].type == DB::TokenType::BareWord
                && tokens[i + 2].type == DB::TokenType::Equals)
            {
                /// Walk the settings list itself - `name = value` pairs separated by commas - and
                /// leave the settings context where the list grammar ends, so nothing after the
                /// list (e.g. a trailing `FORMAT` clause) is scanned as if it were a setting.
                size_t j = i + 1;
                while (j + 1 < tokens.size()
                    && tokens[j].type == DB::TokenType::BareWord
                    && tokens[j + 1].type == DB::TokenType::Equals)
                {
                    const std::string name = toLower(tokens[j].text);
                    j += 2;
                    /// A numeric value may carry a sign (`SETTINGS priority = -1`), which the lexer
                    /// reports as a separate token.
                    if (j + 1 < tokens.size()
                        && (tokens[j].type == DB::TokenType::Minus || tokens[j].type == DB::TokenType::Plus)
                        && tokens[j + 1].type == DB::TokenType::Number)
                        ++j;
                    if (j >= tokens.size())
                        break;
                    if (name == "framing_output_format")
                    {
                        /// A standalone `SET framing_output_format = ...` changes the setting for the
                        /// whole session; the page overrides it on every request, so it is refused
                        /// rather than silently overridden.
                        if (is_set)
                            return {false, false, true};
                        /// A string-literal value carries its surrounding quotes.
                        std::string value = tokens[j].text;
                        if (tokens[j].type == DB::TokenType::StringLiteral && value.size() >= 2)
                            value = value.substr(1, value.size() - 2);
                        saw_query_setting = true;
                        query_setting_disables = toLower(value) == "none";
                    }
                    ++j;
                    if (j < tokens.size() && tokens[j].type == DB::TokenType::Comma)
                        ++j;
                    else
                        break;
                }
                /// Resume the main walk right after the settings list (the loop's `++i` steps past
                /// the last consumed token).
                i = j - 1;
            }
        }
        /// The next token starts a new statement only right after a top-level `;`.
        at_statement_start = is_top_level_semicolon;
    }
    if (saw_query_setting)
    {
        if (query_setting_disables)
            return {false, true, false};
        return {true, false, false};
    }
    return {false, false, false};
}

void expectFraming(const std::string & query, bool user_framing, bool user_disables_framing, bool user_sets_session_framing = false)
{
    const FramingSetting result = detectFramingSetting(query);
    EXPECT_EQ(result.user_framing, user_framing) << "query: " << query;
    EXPECT_EQ(result.user_disables_framing, user_disables_framing) << "query: " << query;
    EXPECT_EQ(result.user_sets_session_framing, user_sets_session_framing) << "query: " << query;
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
    /// An unquoted value is still a real assignment.
    expectFraming("SELECT 1 SETTINGS framing_output_format = EventStream", true, false);
    /// The `SETTINGS` clause may follow the `FORMAT` clause.
    expectFraming("SELECT 1 FORMAT TSV SETTINGS framing_output_format = 'EventStream'", true, false);
}

TEST(PlayDetectFramingSetting, StandaloneSetIsSessionLevel)
{
    /// The reported bug: a standalone `SET framing_output_format = ...` changes the setting for the
    /// whole session (with a `session_id`), which the page cannot honor because it appends its own
    /// `framing_output_format` to every request. It must be reported as a session-level change (so the
    /// page refuses it) rather than as a query-level `user_framing` choice, and never as a plain query.
    expectFraming("SET framing_output_format = 'JSONEachPacketString'", false, false, true);
    expectFraming("set framing_output_format='EventStream'", false, false, true);
    /// A standalone `SET ... = 'None'` is a session-level change too (refused, not treated as an
    /// inline disable).
    expectFraming("SET framing_output_format = 'None'", false, false, true);
    /// The `SET` must be the leading statement keyword: a later statement after a `;` is still checked
    /// per statement.
    expectFraming("SELECT 1; SET framing_output_format = 'JSONEachPacketString'", false, false, true);
    /// A query-level `SETTINGS` clause is NOT a session-level change - it stays a `user_framing` choice.
    expectFraming("SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString'", true, false, false);
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

TEST(PlayDetectFramingSetting, ColumnNamedSettingsDoesNotOpenAContext)
{
    /// The reported bug: a depth-0 bare word `settings` that is just a column reference (followed
    /// by a comma, not by a `name = value` list) must not open a settings context, so the ordinary
    /// comparison after it is not mistaken for a real `framing_output_format` setting.
    expectFraming(
        "SELECT settings, framing_output_format = 'None' FROM values('settings UInt8, framing_output_format String', (1, 'None'))",
        false, false);
    /// An aliased column named `settings` does not open a context either (`settings x` is not
    /// `name = value`).
    expectFraming("SELECT settings x, framing_output_format = 'None' FROM t", false, false);
    /// A real `SETTINGS` clause after such a column reference still wins.
    expectFraming("SELECT settings FROM t SETTINGS framing_output_format = 'EventStream'", true, false);
    /// The settings context ends with the list grammar: a signed numeric value does not break the
    /// walk before a later `framing_output_format` entry in the same list.
    expectFraming("SELECT 1 SETTINGS priority = -1, framing_output_format = 'None'", false, true);
}

TEST(PlayDetectFramingSetting, CommentIsNotASetting)
{
    expectFraming("SELECT 1 -- framing_output_format = None\n", false, false);
    expectFraming("SELECT 1 /* framing_output_format = 'EventStream' */", false, false);
}

TEST(PlayDetectFramingSetting, DuplicateAssignmentsLastWins)
{
    /// The reported bug: the server keeps duplicate `SETTINGS` entries and applies them in order
    /// (`ParserSetQuery` pushes every change, `BaseSettings::applyChanges` walks them sequentially),
    /// so the last assignment is the effective one. The detection must not exit on the first
    /// assignment: here the effective value is `EventStream`, so the query must be treated as a
    /// user framing choice, not refused as a disable.
    expectFraming("SELECT 1 SETTINGS framing_output_format = 'None', framing_output_format = 'EventStream'", true, false);
    /// The reverse order effectively disables framing, so the page must refuse it rather than skip
    /// its own framing and render the plain compact response as raw text.
    expectFraming("SELECT 1 SETTINGS framing_output_format = 'EventStream', framing_output_format = 'None'", false, true);
    /// Three assignments: still the last one wins.
    expectFraming(
        "SELECT 1 SETTINGS framing_output_format = 'None', framing_output_format = 'EventStream', framing_output_format = 'None'",
        false, true);
    /// Other settings interleaved between the duplicates do not change the outcome.
    expectFraming(
        "SELECT 1 SETTINGS framing_output_format = 'None', max_threads = 1, framing_output_format = 'JSONEachPacketString'",
        true, false);
    /// A standalone `SET` is refused as a session-level change regardless of later assignments.
    expectFraming("SET framing_output_format = 'None', framing_output_format = 'EventStream'", false, false, true);
}

TEST(PlayDetectFramingSetting, RealSettingWinsOverStringMention)
{
    /// A real setting alongside a string-literal mention is still detected.
    expectFraming("SELECT 'framing_output_format' SETTINGS framing_output_format = 'None'", false, true);
    expectFraming("SELECT 'framing_output_format = None' SETTINGS framing_output_format = 'EventStream'", true, false);
}
