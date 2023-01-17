#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLProject.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLExtend : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL extend"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
