#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW GRANTS [FOR {user_name [AS USER] | role_name [AS ROLE] | user_and_role_name AS BOTH | CURRENT_USER}]
  */
class ParserShowGrantsQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW GRANTS query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
