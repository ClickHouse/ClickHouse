#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * {GRANT | REVOKE}
  *     role [, role ...]
  *     {TO | FROM} user_or_role [, user_or_role...]
  *     [WITH ADMIN OPTION]
  *
  * {GRANT | REVOKE}
  *     {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
  *     ON *.* | database.* | database.table | * | table
  *     {TO | FROM} user_or_role [, user_or_role ...]
  *     [WITH GRANT OPTION]
  */
class ParserGrantQuery : public IParserBase
{
protected:
    const char * getName() const override { return "GRANT or REVOKE query "; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
