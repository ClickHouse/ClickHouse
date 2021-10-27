#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTUseQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserBackslashLetter.h>


namespace DB
{

bool ParserBackslashLetter::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken t_backslash(TokenType::Backslash);

    if (!t_backslash.ignore(pos, expected))
        return false;

    const char letter = *pos->begin;
    ++pos;

    switch (letter)
    {
    case 'd':
        {
            node = std::make_shared<ASTShowTablesQuery>();
            return true;
        }
    case 'l':
        {
            auto query = std::make_shared<ASTShowTablesQuery>();
            query->databases = true;
            node = query;
            return true;
        }
    case 'c':
        {
            ParserIdentifier name_p;
            ASTPtr database;

            if (!name_p.parse(pos, database, expected))
                return false;

            auto query = std::make_shared<ASTUseQuery>();
            tryGetIdentifierNameInto(database, query->database);
            node = query;
            return true;
        }
    }

    return false;
}


}
