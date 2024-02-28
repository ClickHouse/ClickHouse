#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries of the form
 * SHOW [EXTENDED] INDEX|INDEXES|KEYS FROM|IN tbl [FROM|IN db] [WHERE expr]
 */
class ParserShowIndexesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW INDEXES query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

