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
  *     [DELETE WHERE ...]
  *     [UPDATE col_name = expr, ... WHERE ...]
  */

class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserAlterCommandList : public IParserBase
{
protected:
    const char * getName() const { return "a list of ALTER commands"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserAlterCommand : public IParserBase
{
protected:
    const char * getName() const { return "ALTER command"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/// Part of the UPDATE command of the form: col_name = expr
class ParserAssignment : public IParserBase
{
protected:
    const char * getName() const { return "column assignment"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
