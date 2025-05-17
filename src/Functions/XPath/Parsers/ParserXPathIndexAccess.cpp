#include "ParserXPathIndexAccess.h"

#include <memory>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <Functions/XPath/ASTs/ASTXPathIndexAccess.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserXPathIndexAccess::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::OpeningSquareBracket)
        return false;
    ++pos;

    if (pos->type != TokenType::Number)
    {
        return false;
    }

    auto index_access = std::make_shared<ASTXPathIndexAccess>();
    node = index_access;

    ParserUnsignedInteger integer_p;
    ASTPtr integer_ptr;

    if (!integer_p.parse(pos, integer_ptr, expected))
    {
        return false;
    }
    index_access->index = integer_ptr->as<ASTLiteral>()->value.safeGet<UInt64>();

    if (pos->type != TokenType::ClosingSquareBracket)
    {
        return false;
    }
    ++pos;

    return true;
}

}
