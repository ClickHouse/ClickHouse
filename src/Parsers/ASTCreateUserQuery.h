#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>


namespace DB
{
class ASTExtendedRoleSet;
class ASTSettingsProfileElements;

/** CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *     [NOT IDENTIFIED | IDENTIFIED [WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}]
  *     [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [DEFAULT ROLE role [,...]]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *
  * ALTER USER [IF EXISTS] name
  *      [RENAME TO new_name]
  *      [NOT IDENTIFIED | IDENTIFIED [WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}]
  *      [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *      [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
  *      [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  */
class ASTCreateUserQuery : public IAST, public ASTQueryWithOnCluster
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
    bool show_password = true; /// formatImpl() will show the password or hash.

    std::optional<AllowedClientHosts> hosts;
    std::optional<AllowedClientHosts> add_hosts;
    std::optional<AllowedClientHosts> remove_hosts;

    std::shared_ptr<ASTExtendedRoleSet> default_roles;

    std::shared_ptr<ASTSettingsProfileElements> settings;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateUserQuery>(clone()); }
};
}
