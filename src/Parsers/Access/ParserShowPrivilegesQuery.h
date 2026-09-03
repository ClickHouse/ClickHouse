#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query SHOW PRIVILEGES
  */
class ParserShowPrivilegesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW PRIVILEGES query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
