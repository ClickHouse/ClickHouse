#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * CREATE INDEX [IF NOT EXISTS] name ON [db].name (expression) TYPE type GRANULARITY value
  */

class ParserCreateIndexQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE INDEX query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Parser for index declaration in create index, where name is ignored. */
class ParserCreateIndexDeclaration : public IParserBase
{
public:
    ParserCreateIndexDeclaration() = default;

protected:
    const char * getName() const override { return "index declaration in create index"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
