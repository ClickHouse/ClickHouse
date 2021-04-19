#include <Parsers/ParserTransactionQuery.h>
#include <Parsers/ASTTransactionQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{

bool ParserTransactionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserKeyword("START TRANSACTION").ignore(pos, expected)
        || ParserKeyword("BEGIN").ignore(pos, expected)
        || ParserKeyword("COMMIT").ignore(pos, expected)
        || ParserKeyword("ROLLBACK").ignore(pos, expected))
    {
        node = std::make_shared<ASTTransactionQuery>();
        return true;
    }

    return false;
}

}

