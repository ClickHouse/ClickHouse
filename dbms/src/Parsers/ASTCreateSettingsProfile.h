#pragma once

#include <Parsers/IAST.h>
#include <Access/Authentication.h>
#include <Access/AllowedHosts.h>
#include <Access/SettingsConstraints.h>
#include <Common/SettingsChanges.h>


namespace DB
{
/** CREATE SETTINGS PROFILE [IF NOT EXISTS] name
  *     [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
  *     [USE WITH {role [,...] | NONE | ALL} [EXCEPT role [,...]]]
  *
  * ALTER SETTINGS PROFILE name
  *     [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
  *     [UNSET {varname [,...] | ALL}]
  *     [USE WITH {role [,...] | NONE | ALL} [EXCEPT role [,...]]]
  */
class ASTCreateSettingsProfileQuery : public IAST
{
public:
    bool if_not_exists = false;
    String settings_profile_name;
    bool alter = false;

    SettingsChanges settings;
    SettingsConstraints settings_constraints;
    Strings unset_settings;
    bool unset_all_settings = false;

    struct AppliedRoles
    {
        Strings role_names;
        bool all = false;
        Strings except_names;
    };
    std::optional<AppliedRoles> applied_roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    void formatSettings(const FormatSettings & settings) const;
    void formatAppliedRoles(const FormatSettings & settings) const;
};
}

