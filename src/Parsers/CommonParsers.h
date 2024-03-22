#pragma once

#include <Parsers/IParserBase.h>

#include <cassert>

namespace DB
{

/** Parse specified keyword such as SELECT or compound keyword such as ORDER BY.
  * All case insensitive. Requires word boundary.
  * For compound keywords, any whitespace characters and comments could be in the middle.
  */
/// Example: ORDER/* Hello */BY
class ParserKeyword : public IParserBase
{
private:
    std::string_view s;

public:
    //NOLINTNEXTLINE Want to be able to init ParserKeyword("literal")
    constexpr ParserKeyword(std::string_view s_): s(s_) { assert(!s.empty()); }

    constexpr const char * getName() const override { return s.data(); }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserToken : public IParserBase
{
private:
    TokenType token_type;
public:
    ParserToken(TokenType token_type_) : token_type(token_type_) {} /// NOLINT

protected:
    const char * getName() const override { return "token"; }

    bool parseImpl(Pos & pos, ASTPtr & /*node*/, Expected & expected) override
    {
        if (pos->type != token_type)
        {
            expected.add(pos, getTokenName(token_type));
            return false;
        }
        ++pos;
        return true;
    }
};


// Parser always returns true and do nothing.
class ParserNothing : public IParserBase
{
public:
    const char * getName() const override { return "nothing"; }

    bool parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/) override { return true; }
};

}
