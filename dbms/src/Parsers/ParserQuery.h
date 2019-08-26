#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserQuery : public IParserBase
{
private:
    const char * end;
    bool enable_explain;    /// Allow queries prefixed with AST and ANALYZE for development purposes.

    const char * getName() const override { return "Query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ParserQuery(const char * end_, bool enable_explain_ = false)
        : end(end_),
        enable_explain(enable_explain_)
    {}
};

}
