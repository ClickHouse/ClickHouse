#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserPipelinedQuery : public IParserBase
{
protected:
    bool allow_pipe_syntax;

    const char * getName() const override { return "Pipelined query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserPipelinedQuery(bool allow_pipe_syntax_ = false)
        : allow_pipe_syntax(allow_pipe_syntax_)
    {
    }
};

}
