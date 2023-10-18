#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTShowMergesQuery.h>

namespace DB
{

/** Query SHOW MERGES
  */
class ParserShowMergesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW MERGES query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        auto query = std::make_shared<ASTShowMergesQuery>();

        if (!ParserKeyword("SHOW MERGES").ignore(pos, expected))
            return false;

        node = query;

        return true;
    }
};

}
