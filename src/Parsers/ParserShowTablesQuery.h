#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query like this:
  * SHOW TABLES [FROM db] [[NOT] [I]LIKE 'str'] [LIMIT expr]
  * or
  * SHOW DATABASES.
  */
class ParserShowTablesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW [FULL] [TEMPORARY] TABLES|DATABASES|CLUSTERS|CLUSTER|MERGES 'name' [[NOT] [I]LIKE 'str'] [LIMIT expr]"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
