#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *
  * ALTER ROLE [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  */
class ParserCreateRoleQuery : public IParserBase
{
public:
    ParserCreateRoleQuery & enableAttachMode(bool enable) { attach_mode = enable; return *this; }

protected:
    const char * getName() const override { return "CREATE ROLE or ALTER ROLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
