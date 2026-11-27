#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/** Query of form
 * CHECK [TABLE] [database.]table
 */
class ParserCheckQuery : public IParserBase
{
public:
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const  override{ return "CHECK query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parseCheckTable(Pos & pos, ASTPtr & node, Expected & expected);
    bool parseCheckDatabase(Pos & pos, ASTPtr & node, Expected & expected);
};

}
