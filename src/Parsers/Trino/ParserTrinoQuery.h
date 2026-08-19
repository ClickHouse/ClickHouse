#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses a query written in the Trino SQL dialect.
  *
  * Trino SQL is close to ClickHouse SQL, so instead of a separate grammar the
  * translation works in three stages:
  * 1. Trino-specific syntax (ARRAY[...], TRY_CAST, UNNEST, ROW types, OFFSET
  *    before LIMIT, ...) is rewritten at the token level into ClickHouse
  *    syntax (see TrinoSyntaxTranslator.h);
  * 2. the result is parsed with the standard ClickHouse parser;
  * 3. Trino function names and argument conventions are mapped to ClickHouse
  *    equivalents on the AST (see TrinoFunctionMapper.h).
  *
  * ClickHouse functions that are not Trino names remain accessible, so the
  * dialect can be mixed with native functions when needed.
  *
  * SET queries are handled by the standard parser before the feature gate so
  * that settings like `dialect` can always be changed back, even when
  * `allow_experimental_trino_dialect` is off (recovery from misconfigured
  * profiles). INSERT statements with inline data (VALUES or FORMAT) are
  * delegated to the standard parser as-is, because the data section must keep
  * pointing into the original query buffer.
  */
class ParserTrinoQuery final : public IParserBase
{
private:
    size_t max_query_size;
    size_t max_parser_depth;
    size_t max_parser_backtracks;
    const char * raw_end;
    bool feature_enabled;

public:
    ParserTrinoQuery(
        size_t max_query_size_,
        size_t max_parser_depth_,
        size_t max_parser_backtracks_,
        const char * raw_end_,
        bool feature_enabled_)
        : max_query_size(max_query_size_)
        , max_parser_depth(max_parser_depth_)
        , max_parser_backtracks(max_parser_backtracks_)
        , raw_end(raw_end_)
        , feature_enabled(feature_enabled_)
    {
    }

    const char * getName() const override { return "Trino SQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
