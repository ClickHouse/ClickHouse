#pragma once

#include <Parsers/IAST.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>


namespace DB
{
class ASTExtendedRoleSet;

/** CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *      [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
  *      [HOST {LOCAL | NAME 'name' | NAME REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *      [DEFAULT ROLE role [,...]]
  *      [PROFILE 'profile_name']
  *
  * ALTER USER [IF EXISTS] name
  *      [RENAME TO new_name]
  *      [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
  *      [[ADD|REMOVE] HOST {LOCAL | NAME 'name' | NAME REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *      [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
  *      [PROFILE 'profile_name']
  */
class ASTCreateUserQuery : public IAST
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String name;
    String new_name;

    std::optional<Authentication> authentication;

    std::optional<AllowedClientHosts> hosts;
    std::optional<AllowedClientHosts> add_hosts;
    std::optional<AllowedClientHosts> remove_hosts;

    std::shared_ptr<ASTExtendedRoleSet> default_roles;

    std::optional<String> profile;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
