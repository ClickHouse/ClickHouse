#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW CREATE USER [name | CURRENT_USER]
  * SHOW CREATE USERS [name [, name2 ...]
  * SHOW CREATE ROLE name
  * SHOW CREATE ROLES [name [, name2 ...]]
  * SHOW CREATE [SETTINGS] PROFILE name
  * SHOW CREATE [SETTINGS] PROFILES [name [, name2 ...]]
  * SHOW CREATE [ROW] POLICY name ON [database.]table
  * SHOW CREATE [ROW] POLICIES [name ON [database.]table [, name2 ON database2.table2 ...] | name | ON database.table]
  * SHOW CREATE QUOTA [name]
  * SHOW CREATE QUOTAS [name [, name2 ...]]
  */
class ParserShowCreateAccessEntityQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
