#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace ErrorCodes
{
}


namespace DB
{


bool ParserSystemQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SYSTEM"}.ignore(pos, expected))
        return false;

    using Type = ASTSystemQuery::Type;

    auto res = std::make_shared<ASTSystemQuery>();

    bool found = false;
    for (int i = static_cast<int>(Type::UNKNOWN) + 1; i < static_cast<int>(Type::END); ++i)
    {
        Type t = static_cast<Type>(i);
        if (ParserKeyword{ASTSystemQuery::typeToString(t)}.ignore(pos, expected))
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
        {
            String cluster_str;
            if (ParserKeyword{"ON"}.ignore(pos, expected))
            {
                if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                    return false;
            }
            res->cluster = cluster_str;
            ASTPtr ast;
            if (ParserStringLiteral{}.parse(pos, ast, expected))
                res->target_dictionary = ast->as<ASTLiteral &>().value.safeGet<String>();
            else if (!parseDatabaseAndTableName(pos, expected, res->database, res->target_dictionary))
                return false;
            break;
        }

        case Type::DROP_REPLICA:
        {
            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            res->replica = ast->as<ASTLiteral &>().value.safeGet<String>();
            if (ParserKeyword{"FROM"}.ignore(pos, expected))
            {
                // way 1. parse replica database
                // way 2. parse replica tables
                // way 3. parse replica zkpath
                if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
                {
                    ParserIdentifier database_parser;
                    ASTPtr database;
                    if (!database_parser.parse(pos, database, expected))
                        return false;
                    tryGetIdentifierNameInto(database, res->database);
                }
                else if (ParserKeyword{"TABLE"}.ignore(pos, expected))
                {
                    parseDatabaseAndTableName(pos, expected, res->database, res->table);
                }
                else if (ParserKeyword{"ZKPATH"}.ignore(pos, expected))
                {
                    ASTPtr path_ast;
                    if (!ParserStringLiteral{}.parse(pos, path_ast, expected))
                        return false;
                    String zk_path = path_ast->as<ASTLiteral &>().value.safeGet<String>();
                    if (!zk_path.empty() && zk_path[zk_path.size() - 1] == '/')
                        zk_path.pop_back();
                    res->replica_zk_path = zk_path;
                }
                else
                    return false;
            }
            else
                res->is_drop_whole_replica = true;

            break;
        }

        case Type::RESTART_REPLICA:
        case Type::SYNC_REPLICA:
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                return false;
            break;

        case Type::STOP_DISTRIBUTED_SENDS:
        case Type::START_DISTRIBUTED_SENDS:
        case Type::FLUSH_DISTRIBUTED:
        {
            String cluster_str;
            if (ParserKeyword{"ON"}.ignore(pos, expected))
            {
                if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                    return false;
            }
            res->cluster = cluster_str;
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
            {
                /// FLUSH DISTRIBUTED requires table
                /// START/STOP DISTRIBUTED SENDS does not require table
                if (res->type == Type::FLUSH_DISTRIBUTED)
                    return false;
            }
            break;
        }

        case Type::STOP_MERGES:
        case Type::START_MERGES:
        {
            String storage_policy_str;
            String volume_str;

            if (ParserKeyword{"ON VOLUME"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (ParserIdentifier{}.parse(pos, ast, expected))
                    storage_policy_str = ast->as<ASTIdentifier &>().name();
                else
                    return false;

                if (!ParserToken{TokenType::Dot}.ignore(pos, expected))
                    return false;

                if (ParserIdentifier{}.parse(pos, ast, expected))
                    volume_str = ast->as<ASTIdentifier &>().name();
                else
                    return false;
            }
            res->storage_policy = storage_policy_str;
            res->volume = volume_str;
            if (res->volume.empty() && res->storage_policy.empty())
                parseDatabaseAndTableName(pos, expected, res->database, res->table);
            break;
        }

        case Type::STOP_TTL_MERGES:
        case Type::START_TTL_MERGES:
        case Type::STOP_MOVES:
        case Type::START_MOVES:
        case Type::STOP_FETCHES:
        case Type::START_FETCHES:
        case Type::STOP_REPLICATED_SENDS:
        case Type::START_REPLICATED_SENDS:
        case Type::STOP_REPLICATION_QUEUES:
        case Type::START_REPLICATION_QUEUES:
            parseDatabaseAndTableName(pos, expected, res->database, res->table);
            break;

        default:
            /// There are no [db.table] after COMMAND NAME
            break;
    }

    node = std::move(res);
    return true;
}

}
