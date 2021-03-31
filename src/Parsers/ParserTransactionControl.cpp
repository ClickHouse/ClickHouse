#include <Parsers/ParserTransactionControl.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

bool ParserTransactionControl::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTTransactionControl::QueryType action;

    if (ParserKeyword("BEGIN TRANSACTION").ignore(pos, expected))
        action = ASTTransactionControl::BEGIN;
    else if (ParserKeyword("COMMIT").ignore(pos, expected))
        action = ASTTransactionControl::COMMIT;
    else if (ParserKeyword("ROLLBACK").ignore(pos, expected))
        action = ASTTransactionControl::ROLLBACK;
    else
        return false;

    node = std::make_shared<ASTTransactionControl>(action);
    return true;
}

}
