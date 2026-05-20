#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLCount : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL count"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
