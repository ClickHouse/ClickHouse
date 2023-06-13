#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserPRQLQuery final : public IParserBase
{
private:
    // These fields are not used when PRQL is disabled at build time
    size_t max_query_size; // NOLINT(unused-private-field)
    size_t max_parser_depth; // NOLINT(unused-private-field)

public:
    ParserPRQLQuery(size_t max_query_size_, size_t max_parser_depth_) : max_query_size{max_query_size_}, max_parser_depth{max_parser_depth_}
    {
    }

    const char * getName() const override { return "PRQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
