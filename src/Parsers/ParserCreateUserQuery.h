#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *     [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
  *     [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *
  * ALTER USER [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
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
