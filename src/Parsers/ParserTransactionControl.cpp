#include <Parsers/ParserTransactionControl.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

bool ParserTransactionControl::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTTransactionControl::QueryType action;
    UInt64 snapshot = 0;

    if (ParserKeyword("BEGIN TRANSACTION").ignore(pos, expected))
        action = ASTTransactionControl::BEGIN;
    else if (ParserKeyword("COMMIT").ignore(pos, expected))
        action = ASTTransactionControl::COMMIT;
    else if (ParserKeyword("ROLLBACK").ignore(pos, expected))
        action = ASTTransactionControl::ROLLBACK;
    else if (ParserKeyword("SET TRANSACTION SNAPSHOT").ignore(pos, expected))
    {
        action = ASTTransactionControl::SET_SNAPSHOT;
        ASTPtr ast;
        if (!ParserNumber{}.parse(pos, ast, expected))
            return false;

        const auto & snapshot_num = ast->as<ASTLiteral>()->value;
        if (!snapshot_num.tryGet(snapshot))
            return false;
    }
    else
        return false;

    auto ast = std::make_shared<ASTTransactionControl>(action);
    ast->snapshot = snapshot;
    node = ast;
    return true;
}

}
