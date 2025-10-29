#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTLSQuery.h>


namespace DB
{

/** Query LS
  * Lists files in the current directory
  */
class ParserLSQuery : public IParserBase
{
protected:
    const char * getName() const override { return "LS query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        auto query = std::make_shared<ASTLSQuery>();

        if (!ParserKeyword(Keyword::LS).ignore(pos, expected))
            return false;

        node = query;

        return true;
    }
};

}

