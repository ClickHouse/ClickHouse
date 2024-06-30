#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
// Even when PRQL is disabled, it is not possible to exclude this parser because changing the dialect via `SET dialect = '...'` queries should succeed.
// Another solution would be disabling setting the dialect to PRQL, but it requires a lot of finicky conditional compiling around the Dialect setting enum.
// Therefore the decision, for now, is to use this parser even when PRQL is disabled to enable users to switch to another dialect.
class ParserPRQLQuery final : public IParserBase
{
private:
    // These fields are not used when PRQL is disabled at build time.
    [[maybe_unused]] size_t max_query_size;
    [[maybe_unused]] size_t max_parser_depth;
    [[maybe_unused]] size_t max_parser_backtracks;

public:
    ParserPRQLQuery(size_t max_query_size_, size_t max_parser_depth_, size_t max_parser_backtracks_) : max_query_size(max_query_size_), max_parser_depth(max_parser_depth_), max_parser_backtracks(max_parser_backtracks_)
    {
    }

    const char * getName() const override { return "PRQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
