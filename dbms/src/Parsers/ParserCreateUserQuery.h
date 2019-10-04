#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ASTCreateUserQuery;


/** Parses queries like
  * CREATE USER [IF NOT EXISTS] name
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
class ParserCreateUserQuery : public IParserBase
{
protected:
    const char * getName() const { return "CREATE USER or ALTER USER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool parseAuthentication(Pos & pos, Expected & expected, ASTCreateUserQuery & query);
    bool parseAllowedHosts(Pos & pos, Expected & expected, ASTCreateUserQuery & query);
    bool parseDefaultRoles(Pos & pos, Expected & expected, ASTCreateUserQuery & query);
    bool parseSettings(Pos & pos, Expected & expected, ASTCreateUserQuery & query);
    bool parseAccountLock(Pos & pos, Expected & expected, ASTCreateUserQuery & query);
};
}
