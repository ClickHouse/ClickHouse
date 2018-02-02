#pragma once

#include <Parsers/ParserQueryWithOutput.h>


namespace DB
{

/** Query like this:
  * SHOW TABLES [FROM db] [[NOT] LIKE 'str']
  * or
  * SHOW DATABASES.
  */
class ParserShowTablesQuery : public IParserBase
{
protected:
    const char * getName() const { return "SHOW [TEMPORARY] TABLES|DATABASES [[NOT] LIKE 'str']"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
