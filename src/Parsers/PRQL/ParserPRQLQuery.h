#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserPRQLQuery final : public IParserBase
{
private:
    size_t max_query_size;
    size_t max_parser_depth;

public:
    ParserPRQLQuery(size_t max_query_size_, size_t max_parser_depth_) : max_query_size{max_query_size_}, max_parser_depth{max_parser_depth_}
    {
    }

    const char * getName() const override { return "PRQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
