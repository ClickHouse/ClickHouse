#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Parses query like this:
  * DROP ROLE [IF EXISTS] role [,...]
  */
class ParserDropRoleQuery : public IParserBase
{
protected:
    const char * getName() const { return "DROP ROLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/** Parses query like this:
  * DROP USER [IF EXISTS] user [,...]
  */
class ParserDropUserQuery : public IParserBase
{
protected:
    const char * getName() const { return "DROP USER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};
}
