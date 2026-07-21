#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW USERS
  * SHOW [CURRENT|ENABLED] ROLES
  * SHOW [SETTINGS] PROFILES
  * SHOW [ROW] POLICIES [name | ON [database.]table]
  * SHOW QUOTAS
  * SHOW [CURRENT] QUOTA
  */
class ParserShowAccessEntitiesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ShowAccessEntitiesQuery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
