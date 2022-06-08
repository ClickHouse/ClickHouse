#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Access/Common/AuthenticationData.h>
#include <Access/Common/AllowedClientHosts.h>


namespace DB
{
class ASTUserNamesWithHost;
class ASTRolesOrUsersSet;
class ASTDatabaseOrNone;
class ASTSettingsProfileElements;

/** CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [DEFAULT ROLE role [,...]]
  *     [DEFAULT DATABASE database | NONE]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *     [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
  *
  * ALTER USER [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
  *     [DEFAULT DATABASE database | NONE]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *     [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
  */
class ASTCreateUserQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    std::shared_ptr<ASTUserNamesWithHost> names;
    std::optional<String> new_name;

    std::optional<AuthenticationData> auth_data;
    bool show_password = true; /// formatImpl() will show the password or hash.

    std::optional<AllowedClientHosts> hosts;
    std::optional<AllowedClientHosts> add_hosts;
    std::optional<AllowedClientHosts> remove_hosts;

    std::shared_ptr<ASTRolesOrUsersSet> default_roles;
    std::shared_ptr<ASTSettingsProfileElements> settings;
    std::shared_ptr<ASTRolesOrUsersSet> grantees;

    std::shared_ptr<ASTDatabaseOrNone> default_database;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateUserQuery>(clone()); }
};
}
