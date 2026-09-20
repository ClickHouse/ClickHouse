#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

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

public:
    ParserPolyglotQuery(
        size_t max_query_size_,
        size_t max_parser_depth_,
        size_t max_parser_backtracks_,
        const String & source_dialect_,
        const char * raw_end_,
        bool feature_enabled_)
        : max_query_size(max_query_size_)
        , max_parser_depth(max_parser_depth_)
        , max_parser_backtracks(max_parser_backtracks_)
        , source_dialect(source_dialect_)
        , raw_end(raw_end_)
        , feature_enabled(feature_enabled_)
    {
    }

    const char * getName() const override { return "Polyglot SQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
