#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries of the form
 * SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
 */
class ParserShowFunctionsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW FUNCTIONS query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
