#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [DEFAULT ROLE role [,...]]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *     [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
  *
  * ALTER USER [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
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
