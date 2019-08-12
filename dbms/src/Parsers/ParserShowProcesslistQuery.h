#pragma once

#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>


namespace DB
{
/** Query SHOW PROCESSLIST
  */
class ParserShowProcesslistQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW PROCESSLIST query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        auto query = std::make_shared<ASTShowProcesslistQuery>();

        if (!ParserKeyword("SHOW PROCESSLIST").ignore(pos, expected))
            return false;

        node = query;

        return true;
    }
};

}
