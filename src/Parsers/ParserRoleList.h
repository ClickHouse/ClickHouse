#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses a string like this:
  * {role|CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {role|CURRENT_USER} [,...]
  */
class ParserRoleList : public IParserBase
{
protected:
    const char * getName() const { return "RoleList"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
