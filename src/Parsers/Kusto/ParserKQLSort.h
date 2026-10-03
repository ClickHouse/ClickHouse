#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLSort : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL order by"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
