#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLJoin : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL join"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
