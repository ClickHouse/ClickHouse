#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW [ROW] POLICIES [ON [database.]table]
    SHOW QUOTAS
    SHOW [CURRENT] QUOTA
    SHOW [SETTINGS] PROFILES
  */
class ParserShowAccessEntitiesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ShowAccessEntitiesQuery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
