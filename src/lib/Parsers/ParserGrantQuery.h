#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO user_name
  * REVOKE access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO user_name
  */
class ParserGrantQuery : public IParserBase
{
protected:
    const char * getName() const override { return "GRANT or REVOKE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
