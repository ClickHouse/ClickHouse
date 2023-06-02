#pragma once

#include <Parsers/ASTShowUserProcessesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>


namespace DB
{

/** Query SHOW USER PROCESSES
  */
class ParserShowUserProcessesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW USER PROCESSES query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        auto query = std::make_shared<ASTShowUserProcessesQuery>();

        if (!ParserKeyword("SHOW USER PROCESSES").ignore(pos, expected))
            return false;

        node = query;

        return true;
    }
};

}
