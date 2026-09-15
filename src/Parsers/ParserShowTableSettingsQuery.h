#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// SHOW [CHANGED] TABLE SETTINGS {FROM | IN} [db.]table [[NOT] [I]LIKE 'pattern']
class ParserShowTableSettingsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW TABLE SETTINGS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
