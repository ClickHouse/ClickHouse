#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace DB
{


bool ParserSystemQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SYSTEM"}.ignore(pos))
        return false;

    using Type = ASTSystemQuery::Type;

    auto res = std::make_shared<ASTSystemQuery>();

    bool found = false;
    for (int i = static_cast<int>(Type::UNKNOWN) + 1; i < static_cast<int>(Type::END); ++i)
    {
        Type t = static_cast<Type>(i);
        if (ParserKeyword{ASTSystemQuery::typeToString(t)}.ignore(pos))
        {
            res->type = t;
            found = true;
        }
    }

    if (!found)
        return false;

    switch (res->type)
    {
        case Type::RELOAD_DICTIONARY:
            if (!parseIdentifierOrStringLiteral(pos, expected, res->target_dictionary))
                return false;
            break;

        case Type::RESTART_REPLICA:
        case Type::SYNC_REPLICA:
            if (!parseDatabaseAndTableName(pos, expected, res->target_database, res->target_table))
                return false;
            break;

        case Type::STOP_MERGES:
        case Type::START_MERGES:
        case Type::STOP_FETCHES:
        case Type::START_FETCHES:
        case Type::STOP_REPLICATED_SENDS:
        case Type::START_REPLICATEDS_SENDS:
        case Type::STOP_REPLICATION_QUEUES:
        case Type::START_REPLICATION_QUEUES:
            parseDatabaseAndTableName(pos, expected, res->target_database, res->target_table);
            break;

        default:
            /// There are no [db.table] after COMMAND NAME
            break;
    }

    node = std::move(res);
    return true;
}

}
