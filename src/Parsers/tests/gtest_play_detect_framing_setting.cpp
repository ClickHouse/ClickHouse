#include <gtest/gtest.h>

#include <Parsers/Lexer.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <set>
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
  * keyword must be followed by a settings list - `name = value` pairs, or bare `name` shorthand
  * entries standing for `= true` (`ParserSetQuery` accepts those for `Bool` settings, so
  * `SELECT 1 SETTINGS optimize_move_to_prewhere, framing_output_format = 'None'` is a valid query
  * whose framing choice must be honored) - separated by commas, and the context ends where
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
    /// `max_query_size = 0` means no limit, exactly like the browser's `tokenize`: the page lexes
    /// whatever the editor holds (the server applies its own limits), and a cap here would flag every
    /// token crossing it as an error and silently truncate the token stream of a big query.
    DB::Lexer lexer(query.data(), query.data() + query.size(), 0);
    std::vector<Tok> tokens;
    while (true)
    {
        DB::Token token = lexer.nextToken();
        if (token.isError())
        {
            /// The browser's `tokenize` also stops at an error token, but a port that stopped
            /// silently would analyze a prefix of the query and report the result as if it were
            /// complete - so a truncated analysis fails the test loudly instead.
            ADD_FAILURE() << "the SQL lexer reported an error token: " << DB::getErrorTokenDescription(token.type);
            break;
        }
        if (token.isEnd())
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

/// Mirror of `SETTINGS_LIST_FOLLOWERS` in play.html: the keywords a query-level settings list may
/// run into - the output clauses of `ParserQueryWithOutput` (`FORMAT`, `INTO OUTFILE`) and the data
/// of an `INSERT ... SETTINGS ... VALUES ...`.
const std::set<std::string> & settingsListFollowers()
{
    static const std::set<std::string> followers{"format", "into", "values"};
    return followers;
}

/// Mirror of `settingName` in play.html: `ParserSetQuery` parses the setting name with a full
/// identifier parser, so the name may also be spelled as a quoted identifier - a backquoted
/// `framing_output_format` is the same real setting. Compare by the unquoted, lowercased name;
/// returns an empty optional for a token that cannot be a setting name.
std::optional<std::string> settingName(const Tok & tok)
{
    if (tok.type == DB::TokenType::BareWord)
        return toLower(tok.text);
    if (tok.type == DB::TokenType::QuotedIdentifier && tok.text.size() >= 2)
        return toLower(tok.text.substr(1, tok.text.size() - 2));
    return std::nullopt;
}

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
            /// actual settings list. A list entry is `name = value`, or - `ParserSetQuery` accepts
            /// the shorthand form for `Bool` settings - a bare `name` standing for `name = true`, so
            /// the list may well begin with one. A leading `name = value` entry identifies a settings
            /// list on its own; a leading shorthand entry is much weaker evidence (`SELECT settings
            /// x, ... FROM t` has the same token shape), so such a list is trusted only when it also
            /// ENDS where a settings list can end - see `list_ends_properly` below. A column merely
            /// named `settings` in the query body stays out of the walk either way: it is followed by
            /// a `,` right away, by an implicit alias, or its "list" runs into a `FROM`.
            if ((is_settings || is_set)
                && i + 2 < tokens.size()
                && settingName(tokens[i + 1]).has_value()
                && (tokens[i + 2].type == DB::TokenType::Equals || tokens[i + 2].type == DB::TokenType::Comma))
            {
                /// Walk the settings list itself - `name = value` pairs (or bare `name` shorthand
                /// entries) separated by commas - and leave the settings context where the list
                /// grammar ends, so nothing after the list (e.g. a trailing `FORMAT` clause) is
                /// scanned as if it were a setting.
                const bool opened_by_shorthand = tokens[i + 2].type != DB::TokenType::Equals;
                const bool saved_saw_query_setting = saw_query_setting;
                const bool saved_query_setting_disables = query_setting_disables;
                size_t j = i + 1;
                while (j < tokens.size() && settingName(tokens[j]).has_value())
                {
                    const std::string name = *settingName(tokens[j]);
                    if (j + 1 >= tokens.size() || tokens[j + 1].type != DB::TokenType::Equals)
                    {
                        /// A shorthand entry (`name` with no value, meaning `= true`). It is a list
                        /// entry only where the list continues with a `,` or ends - at the end of the
                        /// query or at a statement-terminating `;`. Anything else ends the list.
                        const bool has_next = j + 1 < tokens.size();
                        const bool continues = has_next && tokens[j + 1].type == DB::TokenType::Comma;
                        if (!continues && has_next && tokens[j + 1].type != DB::TokenType::Semicolon)
                            break;
                        if (name == "framing_output_format")
                        {
                            /// `framing_output_format` is a `String` setting, so the server rejects
                            /// the shorthand form. Still count it as a query-level framing choice:
                            /// the page then adds no framing of its own and the user sees the
                            /// server's own error about the query they wrote.
                            if (is_set)
                                return {false, false, true};
                            saw_query_setting = true;
                            query_setting_disables = false;
                        }
                        ++j;
                        if (continues)
                        {
                            ++j;
                            continue;
                        }
                        break;
                    }
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
                        saw_query_setting = true;
                        if (tokens[j].type == DB::TokenType::OpeningCurlyBrace)
                        {
                            /// A query-parameter placeholder (`= {fmt:String}`): the page cannot know
                            /// what it resolves to (it may well be `None`), so classify it
                            /// conservatively as disabling and refuse the query rather than send a
                            /// request shape the response might not match.
                            query_setting_disables = true;
                        }
                        else
                        {
                            /// A string-literal value carries its surrounding quotes.
                            std::string value = tokens[j].text;
                            if (tokens[j].type == DB::TokenType::StringLiteral && value.size() >= 2)
                                value = value.substr(1, value.size() - 2);
                            const std::string lower_value = toLower(value);
                            /// `ParserSetQuery` also accepts `= DEFAULT` - a reset to the session/server
                            /// default, which the page cannot know (and which is `None` out of the box) -
                            /// so it is classified as disabling too.
                            query_setting_disables = lower_value == "none" || lower_value == "default";
                        }
                    }
                    /// A query-parameter placeholder value (`{name:Type}`) spans several tokens
                    /// (for any setting, not just the framing one); consume it so the walk continues
                    /// at the list grammar after it.
                    if (tokens[j].type == DB::TokenType::OpeningCurlyBrace)
                    {
                        while (j < tokens.size() && tokens[j].type != DB::TokenType::ClosingCurlyBrace)
                            ++j;
                        if (j >= tokens.size())
                            break;
                    }
                    ++j;
                    if (j < tokens.size() && tokens[j].type == DB::TokenType::Comma)
                        ++j;
                    else
                        break;
                }
                /// `tokens[j]` is now the token the list stopped at. A list that was opened by a
                /// shorthand entry alone is trusted only when it ends where a settings list really
                /// can end: at the end of the query, at a statement-terminating `;`, or at one of the
                /// clauses that may follow a settings list. Anything else means the keyword was an
                /// ordinary identifier after all, so the walk is rolled back and this occurrence
                /// contributes nothing.
                const bool list_ends_properly = j >= tokens.size()
                    || tokens[j].type == DB::TokenType::Semicolon
                    || (tokens[j].type == DB::TokenType::BareWord && settingsListFollowers().contains(toLower(tokens[j].text)));
                if (opened_by_shorthand && !list_ends_properly)
                {
                    saw_query_setting = saved_saw_query_setting;
                    query_setting_disables = saved_query_setting_disables;
                }
                else
                {
                    /// Resume the main walk right after the settings list (the loop's `++i` steps past
                    /// the last consumed token).
                    i = j - 1;
                }
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

TEST(PlayDetectFramingSetting, QuotedSettingNameIsARealSetting)
{
    /// The reported bug: `ParserSetQuery` parses the setting name with a full identifier parser, so
    /// a quoted spelling of the name is the same real setting. A detector that requires a bare word
    /// would miss it: the page would then add its own framing while the server honors the quoted
    /// query-level setting, and the response would be dispatched down the wrong branch.
    expectFraming("SELECT 1 SETTINGS `framing_output_format` = 'JSONEachPacketString'", true, false);
    expectFraming("SELECT 1 SETTINGS `framing_output_format` = 'None'", false, true);
    expectFraming("SELECT 1 SETTINGS \"framing_output_format\" = 'EventStream'", true, false);
    /// Case-insensitive like the bare-word spelling, and mixed spellings in one list still walk the
    /// whole list (the last assignment wins).
    expectFraming("SELECT 1 SETTINGS `FRAMING_OUTPUT_FORMAT` = 'None'", false, true);
    expectFraming("SELECT 1 SETTINGS `max_threads` = 1, framing_output_format = 'None'", false, true);
    expectFraming("SELECT 1 SETTINGS framing_output_format = 'None', `framing_output_format` = 'EventStream'", true, false);
    /// The standalone `SET` form is refused as a session-level change for the quoted spelling too.
    expectFraming("SET `framing_output_format` = 'JSONEachPacketString'", false, false, true);
    expectFraming("SET `framing_output_format` = 'None'", false, false, true);
    /// A quoted identifier in the query body is still not a setting.
    expectFraming("SELECT `framing_output_format` = 'None' FROM values('framing_output_format String', ('None'))", false, false);
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

TEST(PlayDetectFramingSetting, NonLiteralValuesAreConservativelyDisabling)
{
    /// The reported bug: `ParserSetQuery` accepts non-literal setting values - `= DEFAULT` (a reset
    /// to the session/server default, which is `None` out of the box) and a query-parameter
    /// placeholder (`= {fmt:String}`, which may resolve to `None`). The page cannot know what
    /// either resolves to, so both must be refused like an explicit `None` instead of being
    /// treated as a user framing choice and sent with a request shape (the compact
    /// `default_format`) that an unframed response would not match.
    expectFraming("SELECT 1 SETTINGS framing_output_format = DEFAULT", false, true);
    expectFraming("select 1 settings framing_output_format = default", false, true);
    expectFraming("SELECT 1 SETTINGS framing_output_format = {fmt:String}", false, true);
    expectFraming("SELECT 1 SETTINGS max_threads = 1, framing_output_format = {fmt:String}", false, true);
    /// The placeholder is consumed by the walk as a whole (`{name:Type}` spans several tokens), so
    /// the list grammar continues after it: a later duplicate assignment still wins...
    expectFraming(
        "SELECT 1 SETTINGS framing_output_format = {fmt:String}, framing_output_format = 'EventStream'",
        true, false);
    expectFraming(
        "SELECT 1 SETTINGS framing_output_format = 'EventStream', framing_output_format = DEFAULT",
        false, true);
    /// ...and a placeholder value of an UNRELATED setting does not end the walk before a later
    /// `framing_output_format` entry in the same list.
    expectFraming(
        "SELECT 1 SETTINGS max_threads = {t:UInt8}, framing_output_format = 'JSONEachPacketString'",
        true, false);
    /// A placeholder in the query body (not in a settings context) is not a setting.
    expectFraming("SELECT {fmt:String}", false, false);
    /// A standalone `SET` is still refused as a session-level change, whatever the value form.
    expectFraming("SET framing_output_format = DEFAULT", false, false, true);
    expectFraming("SET framing_output_format = {fmt:String}", false, false, true);
}

TEST(PlayDetectFramingSetting, ShorthandSettingsInTheListAreConsumed)
{
    /// The reported bug: `ParserSetQuery` accepts valueless shorthand settings (a bare name standing
    /// for `= true`), so a settings list may well begin with one. A walk that only opened a settings
    /// context on a leading `name = value` pair skipped such a list entirely, and the page then added
    /// its own framing instead of honoring - or refusing - the query's own `framing_output_format`.
    expectFraming("SELECT 1 SETTINGS optimize_move_to_prewhere, framing_output_format = 'None'", false, true);
    expectFraming(
        "SELECT 1 SETTINGS optimize_move_to_prewhere, framing_output_format = 'JSONEachPacketString'",
        true, false);
    /// Several shorthand entries in a row, and one after the framing assignment.
    expectFraming(
        "SELECT 1 SETTINGS optimize_move_to_prewhere, allow_experimental_analyzer, framing_output_format = 'None'",
        false, true);
    expectFraming(
        "SELECT 1 SETTINGS framing_output_format = 'None', optimize_move_to_prewhere",
        false, true);
    expectFraming(
        "SELECT 1 SETTINGS optimize_move_to_prewhere, framing_output_format = 'None' FORMAT TSV",
        false, true);
    expectFraming("SELECT 1 SETTINGS optimize_move_to_prewhere, framing_output_format = 'None';", false, true);
    /// A standalone `SET` with a leading shorthand entry is still a session-level change.
    expectFraming("SET optimize_move_to_prewhere, framing_output_format = 'JSONEachPacketString'", false, false, true);
    /// The shorthand form of `framing_output_format` itself is rejected by the server (it is a
    /// `String` setting), but it is still the query's own framing choice, so the page adds none.
    expectFraming("SELECT 1 SETTINGS max_threads = 1, framing_output_format", true, false);
    /// A shorthand list without the framing setting is not a framing choice at all.
    expectFraming("SELECT 1 SETTINGS optimize_move_to_prewhere, allow_experimental_analyzer", false, false);
}

TEST(PlayDetectFramingSetting, ShorthandOpenedListMustEndLikeASettingsList)
{
    /// Understanding shorthand entries must not make an ordinary column named `settings` look like a
    /// settings clause: `SELECT settings x, framing_output_format = 'None' FROM t` has exactly the
    /// token shape of a shorthand-opened list, so it is accepted only if it also ENDS where a
    /// settings list can end. Here it runs into a `FROM`, so the walk is rolled back and the
    /// comparison in the body is not mistaken for a real setting.
    expectFraming("SELECT settings x, framing_output_format = 'None' FROM t", false, false);
    expectFraming("SELECT settings x, framing_output_format = 'None' FROM t WHERE x", false, false);
    /// A real settings clause later in such a query is still found.
    expectFraming("SELECT settings x FROM t SETTINGS optimize_move_to_prewhere, framing_output_format = 'None'", false, true);
    /// The rollback leaves the rest of the walk intact: an unrelated real settings clause after such
    /// a query body is still parsed as one.
    expectFraming("SELECT settings x, framing_output_format = 'None' FROM t SETTINGS max_threads = 1", false, false);
}

TEST(PlayDetectFramingSetting, LargeQueryIsTokenizedWithoutALimit)
{
    /// The reported bug: the browser tokenizer used to cap the lexer at `max_query_size = 65536`,
    /// which flagged every token crossing that boundary as an error and silently truncated the token
    /// stream - so the detection reasoned about a prefix of the query and missed a `SETTINGS` clause
    /// behind it. The page now lexes without a limit (the server applies its own), and so does the
    /// helper above. A padding comment keeps the query valid while pushing the clause past 64 KiB.
    const std::string padding(70000, 'x');
    expectFraming("SELECT 1 /* " + padding + " */ SETTINGS framing_output_format = 'None'", false, true);
    expectFraming(
        "SELECT 1 /* " + padding + " */ SETTINGS framing_output_format = 'JSONEachPacketString'",
        true, false);
    /// A long list of real settings pushes the framing entry past the old cap as well.
    std::string long_list = "SELECT 1 SETTINGS ";
    for (size_t i = 0; i < 3000; ++i)
        long_list += "max_threads = 1, ";
    expectFraming(long_list + "framing_output_format = 'None'", false, true);
    /// A mention past the old cap that is NOT a setting is still not one.
    expectFraming("SELECT '" + padding + " framing_output_format = None'", false, false);
}
