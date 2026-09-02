#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Transpile a query written in a foreign SQL dialect (`source_dialect`, e.g. "postgresql")
/// to ClickHouse SQL using the polyglot-sql library. Throws on transpilation errors or when
/// the input exceeds `max_query_size` (0 disables the limit). The returned string is a
/// standalone ClickHouse query; because it is a real, owned buffer, inline
/// `INSERT ... VALUES`/`FORMAT` data survives parsing (unlike a transient buffer), which is
/// why the server transpiles up front (see executeQuery) rather than inside a parser.
String transpilePolyglotToClickHouse(std::string_view query, std::string_view source_dialect, size_t max_query_size);

/// Transpiles a SQL query from a foreign dialect to ClickHouse SQL using the
/// polyglot-sql library and then parses the result with the standard ClickHouse
/// parser.
///
/// The entire remaining input (from the current position to `raw_end`) is
/// passed to polyglot as an opaque string — it is NOT tokenized by the
/// ClickHouse Lexer, because foreign dialects may contain syntax the Lexer
/// does not understand.  Polyglot handles statement splitting internally.
class ParserPolyglotQuery final : public IParserBase
{
private:
    [[maybe_unused]] size_t max_query_size;
    [[maybe_unused]] size_t max_parser_depth;
    [[maybe_unused]] size_t max_parser_backtracks;
    [[maybe_unused]] String source_dialect;
    [[maybe_unused]] const char * raw_end;
    [[maybe_unused]] bool feature_enabled;
    /// Flags for the standard parser that consumes the transpiled ClickHouse SQL. They must match
    /// the ones the server uses (see `executeQuery`), otherwise the client would reject queries
    /// that the server happily executes.
    [[maybe_unused]] bool allow_settings_after_format_in_insert;
    [[maybe_unused]] bool implicit_select;

public:
    ParserPolyglotQuery(
        size_t max_query_size_,
        size_t max_parser_depth_,
        size_t max_parser_backtracks_,
        const String & source_dialect_,
        const char * raw_end_,
        bool feature_enabled_,
        bool allow_settings_after_format_in_insert_,
        bool implicit_select_)
        : max_query_size(max_query_size_)
        , max_parser_depth(max_parser_depth_)
        , max_parser_backtracks(max_parser_backtracks_)
        , source_dialect(source_dialect_)
        , raw_end(raw_end_)
        , feature_enabled(feature_enabled_)
        , allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
        , implicit_select(implicit_select_)
    {
    }

    const char * getName() const override { return "Polyglot SQL Statement"; }

    /// The input is foreign SQL handed to the transpiler as an opaque string: it may contain tokens
    /// that the ClickHouse Lexer reports as errors (e.g. a bare `!` in `SELECT !0`). The generic
    /// lexical checks of the original buffer must be skipped, otherwise the client would reject
    /// statements that the server (which transpiles before parsing) executes fine.
    bool consumesForeignText() const override { return true; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
