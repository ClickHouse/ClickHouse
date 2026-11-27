#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Parses a user name.
  * It can be a simple string or identifier or something like `name@host`.
  * When `parse_host_pattern` is set (CREATE/ALTER USER), the `@host` part is kept separate from the name.
  */
class ParserUserNameWithHost : public IParserBase
{
public:
    explicit ParserUserNameWithHost(bool allow_query_parameter, bool parse_host_pattern = true);

protected:
    const char * getName() const override { return "UserNameWithHost"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_query_parameter = false;
    bool parse_host_pattern = true;
};


class ParserUserNamesWithHost : public IParserBase
{
public:
    explicit ParserUserNamesWithHost(bool allow_query_parameter, bool parse_host_pattern = true);

protected:
    const char * getName() const override { return "UserNamesWithHost"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_query_parameter = false;
    bool parse_host_pattern = true;
};

}
