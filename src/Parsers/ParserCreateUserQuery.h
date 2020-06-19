#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *     [NOT IDENTIFIED | IDENTIFIED [WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}]
  *     [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *
  * ALTER USER [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [NOT IDENTIFIED | IDENTIFIED [WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}]
  *     [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  */
class ParserCreateUserQuery : public IParserBase
{
public:
    ParserCreateUserQuery & enableAttachMode(bool enable) { attach_mode = enable; return *this; }

protected:
    const char * getName() const override { return "CREATE USER or ALTER USER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
