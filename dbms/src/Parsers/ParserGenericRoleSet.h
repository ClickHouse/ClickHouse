#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses a string like this:
  * {role|CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {role|CURRENT_USER} [,...]
  */
class ParserGenericRoleSet : public IParserBase
{
public:
    ParserGenericRoleSet & allowAll(bool allow_) { allow_all = allow_; return *this; }
    ParserGenericRoleSet & allowCurrentUser(bool allow_) { allow_current_user = allow_; return *this; }

protected:
    const char * getName() const override { return "GenericRoleSet"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_all = true;
    bool allow_current_user = true;
};

}
