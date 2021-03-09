#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{


class ParserExplainQuery : public IParserBase
{
public:
    explicit ParserExplainQuery(bool enable_debug_queries_ = false)
        : enable_debug_queries(enable_debug_queries_)
    {
    }

protected:
    const char * getName() const override { return "EXPLAIN"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool enable_debug_queries;
};

}
