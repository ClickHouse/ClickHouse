#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * DELETE FROM [db.]name WHERE ...
  */

class ParserDeleteQuery : public IParserBase
{
public:
    [[nodiscard]] std::map<String, Documentation> getDocumentation() const override;

protected:
    [[nodiscard]] const char * getName() const override { return "Delete query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
