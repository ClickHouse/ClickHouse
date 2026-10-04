#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
  * SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
  */
class ParserSetRoleQuery : public IParserBase
{
public:
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "SET ROLE or SET DEFAULT ROLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
