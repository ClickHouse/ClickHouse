#include <Parsers/Polyglot/ParserPolyglotQuery.h>

#include "config.h"

#if USE_POLYGLOT
#    include <polyglot.h>
#endif

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTransactionControl.h>
#include <Parsers/Access/ParserSetRoleQuery.h>
#include <Parsers/parseQuery.h>
#include <base/scope_guard.h>
#include <Common/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

String transpilePolyglotToClickHouse(
    [[maybe_unused]] std::string_view query,
    [[maybe_unused]] std::string_view source_dialect,
    [[maybe_unused]] size_t max_query_size)
{
#if !USE_POLYGLOT
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED,
        "Polyglot SQL transpiler is not available. "
        "Rust code or polyglot itself may be disabled. Use another dialect!");
#else
    /// The transpiler must receive the whole foreign query at once, including any inline
    /// `INSERT ... VALUES`/`FORMAT` data (which it rewrites as well), because it cannot know where
    /// the SQL header ends without parsing the foreign dialect. Unlike a native ClickHouse
    /// `INSERT` — whose inline data is streamed and is not bounded by `max_query_size` — a polyglot
    /// query, data included, must therefore fit within `max_query_size`. This is a known limitation
    /// of the experimental dialect: the feature is scoped to inline payloads that fit the parser
    /// size limit. Reject oversized input up front with a dedicated, actionable error (fail-close)
    /// instead of silently truncating it or amplifying memory/CPU usage in the transpiler.
    if (max_query_size && query.size() > max_query_size)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Polyglot query size {} exceeds max_query_size {}. In the polyglot dialect the whole "
            "query is transpiled at once, so any inline INSERT data counts towards max_query_size too "
            "(unlike a native ClickHouse INSERT, whose inline data is streamed and is not subject to "
            "this limit). Increase max_query_size to submit larger inline payloads in this dialect.",
            query.size(), max_query_size);

    uint8_t * sql_query_ptr{nullptr};
    uint64_t sql_query_size{0};

    const auto res = polyglot_transpile(
        reinterpret_cast<const uint8_t *>(query.data()),
        static_cast<uint64_t>(query.size()),
        reinterpret_cast<const uint8_t *>(source_dialect.data()),
        static_cast<uint64_t>(source_dialect.size()),
        &sql_query_ptr,
        &sql_query_size);

    SCOPE_EXIT(
    {
        if (sql_query_ptr)
            polyglot_free_pointer(sql_query_ptr);
    });

    const auto * sql_query_char_ptr = reinterpret_cast<char *>(sql_query_ptr);

    if (res != 0)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Polyglot SQL transpilation error: '{}'",
            sql_query_char_ptr ? std::string_view(sql_query_char_ptr, sql_query_size > 0 ? sql_query_size - 1 : 0) : "unknown error");

    chassert(sql_query_size > 0);

    /// polyglot returns a NUL-terminated string; drop the trailing NUL.
    return String(sql_query_char_ptr, sql_query_size - 1);
#endif
}

namespace
{

/// Words that MySQL and PostgreSQL put right after `SET` and that are not ClickHouse settings:
/// scope modifiers (`SET SESSION sql_mode = ...`, `SET GLOBAL x = 1`, `SET LOCAL x TO 1`) and
/// special forms (`SET NAMES utf8mb4`, `SET CHARACTER SET utf8mb4`). `TIME` is not here: `ParserSetQuery`
/// parses `SET TIME ZONE 'tz'` itself, so a shorthand `SET TIME` followed by junk is a malformed ClickHouse SET.
bool isForeignSetPrefix(const ASTPtr & name)
{
    const auto * identifier = name ? name->as<ASTIdentifier>() : nullptr;
    if (!identifier || identifier->compound())
        return false;

    static constexpr std::string_view prefixes[]
        = {"SESSION", "GLOBAL", "LOCAL", "PERSIST", "PERSIST_ONLY", "NAMES", "CHARACTER"};
    const String & word = identifier->name();
    for (const auto prefix : prefixes)
        if (equalsCaseInsensitive(word, prefix))
            return true;
    return false;
}

}

bool parsePolyglotNativeStatement(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    /// SET queries are standard ClickHouse SQL and must be handled normally
    /// so that settings like `dialect` and `polyglot_dialect` can be changed.
    /// This is checked before the feature gate so users can recover from
    /// misconfigured profiles (e.g. `SET dialect = 'clickhouse'`). Only an input that
    /// unambiguously starts a SET statement is taken from the foreign text, so that the
    /// `SET <setting>` shorthand does not swallow statements merely starting with `set`.
    /// Falling through on failure matters here: ParserSetQuery declines `SET TRANSACTION ...` and
    /// ParserTransactionControl takes only `SET TRANSACTION SNAPSHOT <number>`, so e.g.
    /// `SET TRANSACTION ISOLATION LEVEL ...` still goes to the transpiler. A SET that stops right after the
    /// `SET <word>` shorthand with more input left falls through as well when `<word>` is a foreign
    /// prefix (see `isForeignSetPrefix`): e.g. MySQL `SET SESSION sql_mode = ...` would otherwise be
    /// taken as the shorthand `SET SESSION` (`SESSION = true`) followed by junk. Any other SET that
    /// leaves trailing input, like `SET max_threads = 1 garbage`, `SET max_threads garbage` or
    /// `SET ROLE NONE garbage`, stays a ClickHouse SET, so the caller reports the ordinary syntax
    /// error at the trailing token.
    if (isCommittedToSetQuery(pos))
    {
        const auto set_begin = pos;

        /// SET ROLE / SET DEFAULT ROLE are role statements: ParserSetQuery would take the leading
        /// ROLE / DEFAULT as a setting-name shorthand, so they go first, as in ParserQuery.
        ParserSetRoleQuery set_role_p;
        if (set_role_p.parse(pos, node, expected))
            return true;

        ParserSetQuery set_p;
        if (set_p.parse(pos, node, expected))
        {
            if (pos->isEnd() || pos->type == TokenType::Semicolon)
                return true;

            auto shorthand_end = set_begin;
            Expected shorthand_expected;
            ASTPtr shorthand_name;
            ParserKeyword(Keyword::SET).ignore(shorthand_end, shorthand_expected);
            ParserCompoundIdentifier().parse(shorthand_end, shorthand_name, shorthand_expected);
            if (pos != shorthand_end || !isForeignSetPrefix(shorthand_name))
                return true;

            pos = set_begin;
            node = nullptr;
        }

        /// SET TRANSACTION SNAPSHOT is a transaction statement, which ParserSetQuery declines,
        /// so it goes next, as in ParserQuery.
        ParserTransactionControl transaction_control_p;
        if (transaction_control_p.parse(pos, node, expected))
            return true;
    }

    return false;
}

bool ParserPolyglotQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// See `parsePolyglotNativeStatement`. This is checked before the feature gate so users can recover
    /// from misconfigured profiles (e.g. `SET dialect = 'clickhouse'`).
    if (parsePolyglotNativeStatement(pos, node, expected))
        return true;

    if (!feature_enabled)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Support for polyglot SQL transpiler is disabled (turn on setting 'allow_experimental_polyglot_dialect')");

    if (source_dialect.empty())
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "The `polyglot_dialect` setting must not be empty. "
            "Please specify the source SQL dialect (e.g. 'sqlite', 'mysql', 'postgresql').");

    /// Pass the entire remaining input to polyglot as an opaque string.
    /// Foreign dialects may contain syntax that the ClickHouse Lexer cannot
    /// tokenize correctly, so we do not use the token stream at all.
    const char * begin = pos->begin;
    const std::string_view original_query(begin, static_cast<size_t>(raw_end - begin));

    /// Transpile the foreign SQL to ClickHouse SQL. The transpiled text lives only for
    /// the duration of this function; that is fine here because this parser is used on
    /// the client to classify the query (the server re-transpiles into an owned buffer).
    /// This runs before the token stream is touched: the size guard inside must reject an
    /// oversized query up front, before any tokenization of it.
    const String transpiled = transpilePolyglotToClickHouse(original_query, source_dialect, max_query_size);

    /// Advance the token iterator to the end so the caller knows we consumed all remaining
    /// input. Stop at `ErrorMaxQuerySizeExceeded`, which is terminal: once the stream is past
    /// its size cap the lexer returns it on every call, never reaching `EndOfStream`, so
    /// iterating further would not terminate.
    while (!pos->isEnd() && pos->type != TokenType::ErrorMaxQuerySizeExceeded)
        ++pos;

    /// Parse the transpiled ClickHouse SQL with the standard parser.
    const char * transpiled_begin = transpiled.data();
    const char * const transpiled_end = transpiled.data() + transpiled.size();
    const char * parse_pos = transpiled_begin;
    ParserQuery query_p(transpiled_end, allow_settings_after_format_in_insert, implicit_select);
    String error_message;
    node = tryParseQuery(
        query_p,
        parse_pos,
        transpiled_end,
        error_message,
        false,
        "",
        false,
        max_query_size,
        max_parser_depth,
        max_parser_backtracks,
        true);

    if (!node)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Error while parsing the SQL query generated by polyglot transpiler: '{}'.\n"
            "Original query: '{}'\nTranspiled SQL: '{}'",
            error_message,
            original_query,
            std::string_view(transpiled_begin, transpiled.size()));

    /// An `INSERT ... VALUES`/`FORMAT` statement carries an inline data section after the
    /// SQL text, at which parsing stops; the leftover is that data, not a second statement.
    /// The data pointers reference `transpiled`, which is freed when this function returns,
    /// so clear them: the client sends the original query verbatim and lets the server
    /// re-transpile and read the data from its own owned buffer. This must also cover an
    /// `EXPLAIN INSERT ... VALUES`, whose nested `INSERT` the client dereferences the same way
    /// (`ClientBase::analyzeMultiQueryText`) — otherwise its `data`/`end` would dangle. `getInsertAST`
    /// is the single place that unwraps such carriers; the server side uses it too.
    if (auto * insert = getInsertAST(node); insert && insert->data)
    {
        insert->data = nullptr;
        insert->end = nullptr;
        /// Remember that the statement does carry inline data, even though its pointers are gone: a
        /// caller must not assume that this `INSERT` has a free slot for external data (the server
        /// reads the inline data from its own transpiled buffer). An `INSERT` without inline data —
        /// e.g. `INSERT INTO t FORMAT TSV` — leaves the flag unset and keeps the ordinary streaming
        /// path, exactly like a native `INSERT`.
        insert->inline_data_owned_by_transpiled_query = true;
        return true;
    }

    /// Reject multi-statement input: if the transpiled SQL contains more
    /// than one statement, `tryParseQuery` only parses the first one and
    /// silently dropping the rest would be surprising.  Detect leftover
    /// non-whitespace content after the parsed statement.
    /// Note: `tryParseQuery` advances `parse_pos` past the parsed statement.
    while (parse_pos < transpiled_end
           && (*parse_pos == ' ' || *parse_pos == '\t' || *parse_pos == '\r' || *parse_pos == '\n'))
        ++parse_pos;
    if (parse_pos < transpiled_end)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Multi-statement queries are not supported in polyglot dialect mode. "
            "Please submit one statement at a time.");

    return true;
}

}
