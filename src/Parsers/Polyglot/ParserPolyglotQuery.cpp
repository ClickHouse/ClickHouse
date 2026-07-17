#include <Parsers/Polyglot/ParserPolyglotQuery.h>

#include "config.h"

#if USE_POLYGLOT
#    include <polyglot.h>
#endif

#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Return the `INSERT` that owns the inline-data section of a parsed statement, if any.
/// The client locates the inline-data boundary the same way in `ClientBase::analyzeMultiQueryText`:
/// it also unwraps a single `EXPLAIN` layer, so that `EXPLAIN INSERT ... VALUES (...)` is handled
/// like a plain `INSERT`. We must therefore recognize the explained `INSERT` here too.
ASTInsertQuery * findInlineDataInsert(IAST * node)
{
    if (auto * insert = node->as<ASTInsertQuery>())
        return insert;
    if (auto * explain = node->as<ASTExplainQuery>(); explain && explain->getExplainedQuery())
        return explain->getExplainedQuery()->as<ASTInsertQuery>();
    return nullptr;
}

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

bool ParserPolyglotQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// SET queries are standard ClickHouse SQL and must be handled normally
    /// so that settings like `dialect` and `polyglot_dialect` can be changed.
    /// This is checked before the feature gate so users can recover from
    /// misconfigured profiles (e.g. `SET dialect = 'clickhouse'`).
    ParserSetQuery set_p;
    if (set_p.parse(pos, node, expected))
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

    /// Advance the token iterator to the end so the caller knows we
    /// consumed all remaining input.
    while (!pos->isEnd())
        ++pos;

    const std::string_view original_query(begin, static_cast<size_t>(raw_end - begin));

    /// Transpile the foreign SQL to ClickHouse SQL. The transpiled text lives only for
    /// the duration of this function; that is fine here because this parser is used on
    /// the client to classify the query (the server re-transpiles into an owned buffer).
    const String transpiled = transpilePolyglotToClickHouse(original_query, source_dialect, max_query_size);

    /// Parse the transpiled ClickHouse SQL with the standard parser.
    const char * transpiled_begin = transpiled.data();
    const char * const transpiled_end = transpiled.data() + transpiled.size();
    const char * parse_pos = transpiled_begin;
    ParserQuery query_p(transpiled_end, false);
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
    /// (`ClientBase::analyzeMultiQueryText`) — otherwise its `data`/`end` would dangle.
    if (auto * insert = findInlineDataInsert(node.get()); insert && insert->data)
    {
        insert->data = nullptr;
        insert->end = nullptr;
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
