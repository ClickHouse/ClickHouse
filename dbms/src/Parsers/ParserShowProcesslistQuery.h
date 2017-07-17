#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTShowProcesslistQuery.h>


namespace DB
{

/** Query SHOW PROCESSLIST
  */
class ParserShowProcesslistQuery : public IParserBase
{
protected:
    const char * getName() const { return "SHOW PROCESSLIST query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        Pos begin = pos;

        auto query = std::make_shared<ASTShowProcesslistQuery>();

        if (!ParserKeyword("SHOW PROCESSLIST").ignore(pos, expected))
            return false;

        query->range = StringRange(begin, pos);
        node = query;

        return true;
    }
};

}
