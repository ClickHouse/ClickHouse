#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLTable : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL Table"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool parsePrepare(Pos &pos) override;

};

}
