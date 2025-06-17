#pragma once

#include <Parsers/Kusto/IKQLParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLSummarize : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL summarize"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};

}
