#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTModifyEngineQuery.h>

namespace DB
{

/** Query like this:
  * ALTER TABLE [db.]name [ON CLUSTER cluster] MODIFY
  * ENGINE = name [PARTITION BY expr] [ORDER BY expr] [PRIMARY KEY expr] [SAMPLE BY expr] [SETTINGS name = value, ...]
  */

class ParserModifyEngineQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "ALTER TABLE MODIFY ENGINE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
