#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Parses a user name.
  * It can be a simple string or identifier or something like `name@host`.
  */
class ParserUserNameWithHost : public IParserBase
{
public:
    explicit ParserUserNameWithHost(bool allow_query_parameter);

protected:
    const char * getName() const override { return "UserNameWithHost"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_query_parameter = false;
};


class ParserUserNamesWithHost : public IParserBase
{
public:
    explicit ParserUserNamesWithHost(bool allow_query_parameter);

protected:
    const char * getName() const override { return "UserNamesWithHost"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_query_parameter = false;
};

}
