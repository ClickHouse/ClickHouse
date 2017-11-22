#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

/** Query like this:
  * ALTER TABLE [db.]name [ON CLUSTER cluster]
  *     [ADD COLUMN col_name type [AFTER col_after],]
  *     [DROP COLUMN col_to_drop, ...]
  *     [CLEAR COLUMN col_to_clear [IN PARTITION partition],]
  *     [MODIFY COLUMN col_to_modify type, ...]
  *     [MODIFY PRIMARY KEY (a, b, c...)]
  *     [DROP|DETACH|ATTACH PARTITION|PART partition, ...]
  *     [FETCH PARTITION partition FROM ...]
  *     [FREEZE PARTITION]
  */
class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
