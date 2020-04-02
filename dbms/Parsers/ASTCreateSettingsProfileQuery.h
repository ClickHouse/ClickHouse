#pragma once

#include <Parsers/IAST.h>


namespace DB
{
class ASTSettingsProfileElements;
class ASTExtendedRoleSet;


/** CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *
  * ALTER SETTINGS PROFILE [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  */
class ASTCreateSettingsProfileQuery : public IAST
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String name;
    String new_name;

    std::shared_ptr<ASTSettingsProfileElements> settings;

    std::shared_ptr<ASTExtendedRoleSet> to_roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};
}
