#include "ParserXPathAttribute.h"

#include <memory>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <Functions/XPath/ASTs/ASTXPathAttribute.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserXPathAttribute::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::OpeningSquareBracket)
        return false;
    ++pos;

    if (pos->type != TokenType::At)
        return false;
    ++pos;

    if (pos->type != TokenType::BareWord)
        return false;

    ASTPtr attribute_name;
    ParserIdentifier attribute_name_p;
    if (!attribute_name_p.parse(pos, attribute_name, expected))
        return false;

    auto attribute = std::make_shared<ASTXPathAttribute>();
    node = attribute;

    if (!tryGetIdentifierNameInto(attribute_name, attribute->name))
    {
        return false;
    }

    // TODO: add <, <=, >, >= ?
    if (pos->type == TokenType::ClosingSquareBracket)
    {
        attribute->value = String{};
        ++pos;
        return true;
    }
    else if (pos->type == TokenType::Equals)
    {
        ++pos;
    }
    else
    {
        return false;
    }

    ASTPtr attribute_value;
    if (pos->type == TokenType::BareWord || pos->type == TokenType::QuotedIdentifier)
    {
        ParserIdentifier attribute_value_p;
        if (!attribute_value_p.parse(pos, attribute_value, expected))
            return false;
    }
    else if (pos->type == TokenType::StringLiteral)
    {
        ReadBufferFromMemory in(pos->begin, pos->size());
        String value;
        readQuotedString(value, in);
        attribute_value = std::make_shared<ASTIdentifier>(value);
        ++pos;
    }
    else
    {
        return false;
    }

    if (!tryGetIdentifierNameInto(attribute_value, attribute->value))
    {
        return false;
    }

    if (pos->type != TokenType::ClosingSquareBracket)
    {
        return false;
    }
    ++pos;

    return true;
}

}
