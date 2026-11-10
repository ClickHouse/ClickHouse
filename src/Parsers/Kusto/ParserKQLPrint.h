#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLPrint : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL project"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
