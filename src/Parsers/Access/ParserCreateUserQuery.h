#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <Parsers/IParserBase.h>


namespace DB
{

/** Parses the `VALID UNTIL <datetime>` / `VALID FOR <interval>` clause of an authentication method.
  * `is_interval` is set to true for the `VALID FOR` form, whose deadline is resolved as `now` plus
  * the parsed interval when the query is executed. Shared with `CREATE TOKEN`.
  */
bool parseAuthenticationValidUntil(IParserBase::Pos & pos, Expected & expected, ASTPtr & valid_until, bool & is_interval);

/** Parses the `GRANTS (privilege ON object [,...])` clause of an authentication method,
  * which limits the access rights of the sessions authenticated with it. Shared with `CREATE TOKEN`.
  */
bool parseAuthenticationGrants(IParserBase::Pos & pos, Expected & expected, AccessRightsElements & grants);

/** Parses queries like
  * CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [DEFAULT ROLE role [,...]]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
  *     [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
  *
  * ALTER USER [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
  *     [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
  *     [DROP SETTINGS variable [,...] ]
  *     [ADD PROFILES 'profile_name' [,...] ]
  *     [DROP PROFILES 'profile_name' [,...] ]
  *     [DROP ALL PROFILES]
  *     [DROP ALL SETTINGS]
  *     [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
  */
class ParserCreateUserQuery : public IParserBase
{
public:
    ParserCreateUserQuery & useAttachMode(bool attach_mode_ = true) { attach_mode = attach_mode_; return *this; }

protected:
    const char * getName() const override { return "CREATE USER or ALTER USER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
