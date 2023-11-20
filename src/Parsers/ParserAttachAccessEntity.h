#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Special parser for the 'ATTACH access entity' queries.
class ParserAttachAccessEntity : public IParserBase
{
protected:
    const char * getName() const override { return "ATTACH access entity query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

};

}
