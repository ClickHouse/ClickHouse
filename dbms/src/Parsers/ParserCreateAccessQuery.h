#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/// Parses a string like
///     IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
class ParserAuthentication : public IParserBase
{
protected:
    const char * getName() const { return "authentication options"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/// Parses a string like
///     [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | NONE | ANY }]
class ParserAllowedHosts : public IParserBase
{
protected:
    const char * getName() const { return "allowed hosts"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/// Parses a string like
///     [DEFAULT ROLE {role[,...] | NONE}]
class ParserDefaultRoles : public IParserBase
{
public:
    ParserDefaultRoles(bool alter_ = false) : alter(alter_) {}
protected:
    const char * getName() const { return "default roles"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool alter;
};


/** CREATE ROLE [IF NOT EXISTS] name [,...]
  */
class ParserCreateRoleQuery : public IParserBase
{
protected:
    const char * getName() const { return "CREATE ROLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** CREATE USER [IF NOT EXISTS] name
  *     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
  *     [HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY}]
  *     [DEFAULT ROLE {role[,...] | NONE}]
  *     [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
  *     [ACCOUNT {LOCK | UNLOCK}]
  */
class ParserCreateUserQuery : public IParserBase
{
protected:
    const char * getName() const { return "CREATE USER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
