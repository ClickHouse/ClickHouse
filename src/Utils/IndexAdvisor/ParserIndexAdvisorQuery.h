#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserIndexAdvisorQuery : public IParserBase
{
protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    const char * getName() const override { return "ParserIndexAdvisorQuery"; }
};

} 
