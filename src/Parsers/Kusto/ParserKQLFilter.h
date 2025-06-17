#pragma once

#include <Parsers/Kusto/IKQLParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLFilter : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL where"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};

}
