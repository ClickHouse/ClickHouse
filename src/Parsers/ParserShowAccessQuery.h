#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTShowAccessQuery.h>


namespace DB
{

/** Query SHOW ACCESS
  */
class ParserShowAccessQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW ACCESS query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        auto query = std::make_shared<ASTShowAccessQuery>();

        if (!ParserKeyword("SHOW ACCESS").ignore(pos, expected))
            return false;

        node = query;

        return true;
    }
};

}
