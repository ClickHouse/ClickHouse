#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * MOVE {USER | ROLE | QUOTA | [ROW] POLICY | [SETTINGS] PROFILE} [IF EXISTS] name [,...] [ON [database.]table [,...]] TO storage_name
  */
class ParserMoveAccessEntityQuery : public IParserBase
{
protected:
    const char * getName() const override { return "MOVE access entity query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
