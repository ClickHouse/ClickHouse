#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
  *
  * ALTER SETTINGS PROFILE [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
  */
class ParserCreateSettingsProfileQuery : public IParserBase
{
public:
    ParserCreateSettingsProfileQuery & enableAttachMode(bool enable) { attach_mode = enable; return *this; }

protected:
    const char * getName() const override { return "CREATE SETTINGS PROFILE or ALTER SETTINGS PROFILE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
