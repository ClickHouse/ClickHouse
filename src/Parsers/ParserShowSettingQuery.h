#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries of the form:
  * SHOW SETTING [setting_name]
  */
class ParserShowSettingQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW SETTING query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

