#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses a user name.
  * It can be a simple string or identifier or something like `name@host`.
  */
class ParserUserNameWithHost : public IParserBase
{
protected:
    const char * getName() const override { return "UserNameWithHost"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserUserNamesWithHost : public IParserBase
{
protected:
    const char * getName() const override { return "UserNamesWithHost"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
