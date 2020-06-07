#pragma once

#include <Parsers/IParserBase.h>

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
    const char * s;

public:
    ParserKeyword(const char * s_);

protected:
    const char * getName() const override;

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges) override;

    const char * color() const override { return IAST::hilite_keyword; }
};


class ParserToken : public IParserBase
{
private:
    TokenType token_type;
public:
    ParserToken(TokenType token_type_) : token_type(token_type_) {}
protected:
    const char * getName() const override { return "token"; }

    bool parseImpl(Pos & pos, ASTPtr & /*node*/, Expected & expected, Ranges *) override
    {
        if (pos->type != token_type)
        {
            expected.add(pos, getTokenName(token_type));
            return false;
        }
        ++pos;
        return true;
    }

    const char * color() const override
    {
        switch (token_type)
        {
            case TokenType::Whitespace:
            case TokenType::Comment:
                return IAST::hilite_none;

            case TokenType::Number:
            case TokenType::StringLiteral:
                return IAST::hilite_none;

            case TokenType::QuotedIdentifier:

            case TokenType::OpeningRoundBracket:
            case TokenType::ClosingRoundBracket:

            case TokenType::OpeningSquareBracket:
            case TokenType::ClosingSquareBracket:

            case TokenType::OpeningCurlyBrace:
            case TokenType::ClosingCurlyBrace:

            case TokenType::Comma:
            case TokenType::Semicolon:
            case TokenType::Dot:
                return IAST::hilite_none;

            case TokenType::Asterisk:
                break;

            case TokenType::Plus:
            case TokenType::Minus:
            case TokenType::Slash:
            case TokenType::Percent:
            case TokenType::Arrow:
            case TokenType::QuestionMark:
            case TokenType::Colon:
            case TokenType::Equals:
            case TokenType::NotEquals:
            case TokenType::Less:
            case TokenType::Greater:
            case TokenType::LessOrEquals:
            case TokenType::GreaterOrEquals:
            case TokenType::Concatenation:
            case TokenType::At:
                return IAST::hilite_operator;

            default:
                return nullptr;
        }

        __builtin_unreachable();
    }
};


// Parser always returns true and do nothing.
class ParserNothing : public IParserBase
{
public:
    const char * getName() const override { return "nothing"; }

    bool parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/, Ranges *) override { return true; }
};

}
