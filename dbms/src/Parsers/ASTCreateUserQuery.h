#pragma once

#include <Parsers/IAST.h>
#include <Access/AllowedHosts.h>
#include <Access/SettingsConstraints.h>
#include <Common/SettingsChanges.h>


namespace DB
{
/** CREATE USER [IF NOT EXISTS] name
  *     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
  *     [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY}]
  *     [DEFAULT ROLE {role[,...] | NONE}]
  *     [SETTINGS varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
  *     [ACCOUNT {LOCK | UNLOCK}]
  *
  * ALTER USER name
  *     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
  *     [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY}]
  *     [DEFAULT ROLE {role[,...] | NONE}]
  *     [SETTINGS varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
  *     [ACCOUNT {LOCK | UNLOCK}]
  */
class ASTCreateUserQuery : public IAST
{
public:
    bool if_not_exists = false;
    String user_name;
    bool alter = false;

    struct Authentication
    {
        enum Type
        {
            NO_PASSWORD,
            PLAINTEXT_PASSWORD,
            SHA256_PASSWORD,
            SHA256_HASH,
        };
        Type type = NO_PASSWORD;
        ASTPtr password;
    };
    std::optional<Authentication> authentication;

    std::optional<AllowedHosts> allowed_hosts;

    struct DefaultRoles
    {
        Strings role_names;
        bool all_granted = false;
    };
    std::optional<DefaultRoles> default_roles;

    std::optional<SettingsChanges> settings;
    std::optional<SettingsConstraints> settings_constraints;

    struct AccountLock
    {
        bool locked = false;
    };
    std::optional<AccountLock> account_lock;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    void formatAuthentication(const FormatSettings & settings) const;
    void formatAllowedHosts(const FormatSettings & settings) const;
    void formatDefaultRoles(const FormatSettings & settings) const;
    void formatSettings(const FormatSettings & settings) const;
    void formatAccountLock(const FormatSettings & settings) const;
};
}

