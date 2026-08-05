#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CHECK GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}
  */
class ParserCheckGrantQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CHECK GRANT"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
