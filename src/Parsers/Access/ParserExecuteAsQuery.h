#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like :
  * EXECUTE AS <user>
  *   or
  * EXECUTE AS <user> <subquery>
  */
class ParserExecuteAsQuery : public IParserBase
{
public:
    explicit ParserExecuteAsQuery(IParser & subquery_parser_);
    const char * getName() const override { return "EXECUTE AS query"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    IParser & subquery_parser;
};

}
