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
    void useAttachMode(bool attach_mode_ = true) { attach_mode = attach_mode_; }

protected:
    const char * getName() const override { return "CREATE SETTINGS PROFILE or ALTER SETTINGS PROFILE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
