#include <Parsers/MySQL/ParserMySQLIdentifier.h>

#include <Parsers/Lexer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace MySQLParser
{

bool ParserMySQLIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & /*expected*/)
{
    /// Identifier in backquotes or in double quotes
    if (pos->type == TokenType::QuotedIdentifier)
    {
        ReadBufferFromMemory buf(pos->begin, pos->size());
        String s;

        if (*pos->begin == '`')
            readBackQuotedStringWithSQLStyle(s, buf);
        else
            readDoubleQuotedStringWithSQLStyle(s, buf);

        if (s.empty())    /// Identifiers "empty string" are not allowed.
            return false;

        node = std::make_shared<ASTIdentifier>(s);
        ++pos;
        return true;
    }
    else if (pos->type == TokenType::BareWord)
    {
        node = std::make_shared<ASTIdentifier>(String(pos->begin, pos->end));
        ++pos;
        return true;
    }
    else if (pos->type == TokenType::ErrorWrongNumber)
    {
        for (const char * iterator = pos->begin; iterator != pos->end; ++iterator)
        {
            if (!isWordCharASCII(*iterator))
                return false;
        }

        node = std::make_shared<ASTIdentifier>(String(pos->begin, pos->end));
        ++pos;
        return true;
    }

    return false;
}

}

}
