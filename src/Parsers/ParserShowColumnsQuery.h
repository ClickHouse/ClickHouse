#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries of the form
 * SHOW [EXTENDED] [FULL] COLUMNS FROM|IN tbl [FROM|IN db] [[NOT] LIKE|ILIKE expr | WHERE expr] [LIMIT n]
 */
class ParserShowColumnsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW COLUMNS query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
