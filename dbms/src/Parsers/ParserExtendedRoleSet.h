#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses a string like this:
  * {role|CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {role|CURRENT_USER} [,...]
  */
class ParserExtendedRoleSet : public IParserBase
{
public:
    ParserExtendedRoleSet & enableAllKeyword(bool enable_) { all_keyword = enable_; return *this; }
    ParserExtendedRoleSet & enableCurrentUserKeyword(bool enable_) { current_user_keyword = enable_; return *this; }
    ParserExtendedRoleSet & useIDMode(bool enable_) { id_mode = enable_; return *this; }

protected:
    const char * getName() const override { return "ExtendedRoleSet"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool all_keyword = true;
    bool current_user_keyword = true;
    bool id_mode = false;
};

}
