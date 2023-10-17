#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

namespace MySQLParser
{

class ParserDeclareTableOptions : public IParserBase
{
protected:
    const char * getName() const override { return "table options declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
