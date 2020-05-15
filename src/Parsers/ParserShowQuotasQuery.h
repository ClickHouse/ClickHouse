#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW QUOTAS
  * SHOW QUOTA USAGE [CURRENT | ALL]
  */
class ParserShowQuotasQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
