#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>

#include <magic_enum.hpp>
#include <base/EnumReflection.h>

namespace ErrorCodes
{
}


namespace DB
{

[[nodiscard]] static bool parseQueryWithOnClusterAndMaybeTable(std::shared_ptr<ASTSystemQuery> & res, IParser::Pos & pos,
                                                 Expected & expected, bool require_table, bool allow_string_literal)
{
    /// Better form for user: SYSTEM <ACTION> table ON CLUSTER cluster
    /// Query rewritten form + form while executing on cluster: SYSTEM <ACTION> ON CLUSTER cluster table
    /// Need to support both
    String cluster;
    bool parsed_on_cluster = false;

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
        parsed_on_cluster = true;
    }

    bool parsed_table = false;
    if (allow_string_literal)
    {
        ASTPtr ast;
        if (ParserStringLiteral{}.parse(pos, ast, expected))
        {
            res->setTable(ast->as<ASTLiteral &>().value.safeGet<String>());
            parsed_table = true;
        }
    }

    if (!parsed_table)
        parsed_table = parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);

    if (!parsed_table && require_table)
        return false;

    if (!parsed_on_cluster && ParserKeyword{"ON"}.ignore(pos, expected))
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;

    res->cluster = cluster;

    if (res->database)
        res->children.push_back(res->database);
    if (res->table)
        res->children.push_back(res->table);

    return true;
}

enum class SystemQueryTargetType
{
    Model,
    Function,
    Disk,
};

[[nodiscard]] static bool parseQueryWithOnClusterAndTarget(std::shared_ptr<ASTSystemQuery> & res, IParser::Pos & pos, Expected & expected, SystemQueryTargetType target_type)
{
    /// Better form for user: SYSTEM <ACTION> target_name ON CLUSTER cluster
    /// Query rewritten form + form while executing on cluster: SYSTEM <ACTION> ON CLUSTER cluster target_name
    /// Need to support both

    String cluster;
    bool parsed_on_cluster = false;

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
        parsed_on_cluster = true;
    }

    String target;
    ASTPtr temporary_string_literal;

    if (ParserStringLiteral{}.parse(pos, temporary_string_literal, expected))
    {
        target = temporary_string_literal->as<ASTLiteral &>().value.safeGet<String>();
    }
    else
    {
        ParserIdentifier identifier_parser;
        ASTPtr identifier;

        if (!identifier_parser.parse(pos, identifier, expected))
            return false;

        if (!tryGetIdentifierNameInto(identifier, target))
            return false;
    }

    if (!parsed_on_cluster && ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
    }

    res->cluster = cluster;

    switch (target_type)
    {
        case SystemQueryTargetType::Model:
        {
            res->target_model = std::move(target);
            break;
        }
        case SystemQueryTargetType::Function:
        {
            res->target_function = std::move(target);
            break;
        }
        case SystemQueryTargetType::Disk:
        {
            res->disk = std::move(target);
            break;
        }
    }

    return true;
}

[[nodiscard]] static bool parseQueryWithOnCluster(std::shared_ptr<ASTSystemQuery> & res, IParser::Pos & pos,
                                    Expected & expected)
{
    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }
    res->cluster = cluster_str;

    return true;
}

[[nodiscard]] static bool parseDropReplica(std::shared_ptr<ASTSystemQuery> & res, IParser::Pos & pos, Expected & expected, bool database)
{
    if (!parseQueryWithOnCluster(res, pos, expected))
        return false;

    ASTPtr ast;
    if (!ParserStringLiteral{}.parse(pos, ast, expected))
        return false;
    res->replica = ast->as<ASTLiteral &>().value.safeGet<String>();

    if (ParserKeyword{"FROM SHARD"}.ignore(pos, expected))
    {
        if (!ParserStringLiteral{}.parse(pos, ast, expected))
            return false;
        res->shard = ast->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (ParserKeyword{"FROM"}.ignore(pos, expected))
    {
        // way 1. parse replica database
        // way 2. parse replica table
        // way 3. parse replica zkpath
        if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
        {
            ParserIdentifier database_parser;
            if (!database_parser.parse(pos, res->database, expected))
                return false;
        }
        else if (!database && ParserKeyword{"TABLE"}.ignore(pos, expected))
        {
            parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
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

    return true;
}

bool ParserSystemQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SYSTEM"}.ignore(pos, expected))
        return false;

    using Type = ASTSystemQuery::Type;

    auto res = std::make_shared<ASTSystemQuery>();

    bool found = false;

    for (const auto & type : magic_enum::enum_values<Type>())
    {
        if (ParserKeyword{ASTSystemQuery::typeToString(type)}.ignore(pos, expected))
        {
            res->type = type;
            found = true;
            break;
        }
    }

    if (!found)
        return false;

    switch (res->type)
    {
        case Type::RELOAD_DICTIONARY:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ true))
                return false;
            break;
        }
        case Type::RELOAD_MODEL:
        {
            if (!parseQueryWithOnClusterAndTarget(res, pos, expected, SystemQueryTargetType::Model))
                return false;
            break;
        }
        case Type::RELOAD_FUNCTION:
        {
            if (!parseQueryWithOnClusterAndTarget(res, pos, expected, SystemQueryTargetType::Function))
                return false;
            break;
        }

        case Type::DROP_REPLICA:
        {
            if (!parseDropReplica(res, pos, expected, /* database */ false))
                return false;
            break;
        }
        case Type::DROP_DATABASE_REPLICA:
        {
            if (!parseDropReplica(res, pos, expected, /* database */ true))
                return false;
            break;
        }
        case Type::ENABLE_FAILPOINT:
        case Type::DISABLE_FAILPOINT:
        {
            ASTPtr ast;
            if (ParserIdentifier{}.parse(pos, ast, expected))
                res->fail_point_name = ast->as<ASTIdentifier &>().name();
            else
                return false;
            break;
        }

        case Type::RESTART_REPLICA:
        case Type::SYNC_REPLICA:
        case Type::WAIT_LOADING_PARTS:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;
            if (res->type == Type::SYNC_REPLICA)
            {
                if (ParserKeyword{"STRICT"}.ignore(pos, expected))
                    res->sync_replica_mode = SyncReplicaMode::STRICT;
                else if (ParserKeyword{"LIGHTWEIGHT"}.ignore(pos, expected))
                    res->sync_replica_mode = SyncReplicaMode::LIGHTWEIGHT;
                else if (ParserKeyword{"PULL"}.ignore(pos, expected))
                    res->sync_replica_mode = SyncReplicaMode::PULL;
            }
            break;
        }

        case Type::SYNC_DATABASE_REPLICA:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            if (!parseDatabaseAsAST(pos, expected, res->database))
                return false;
            break;
        }
        case Type::RESTART_DISK:
        {
            if (!parseQueryWithOnClusterAndTarget(res, pos, expected, SystemQueryTargetType::Disk))
                return false;
            break;
        }
        /// FLUSH DISTRIBUTED requires table
        /// START/STOP DISTRIBUTED SENDS does not require table
        case Type::STOP_DISTRIBUTED_SENDS:
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ false, /* allow_string_literal = */ false))
                return false;
            break;
        }

        case Type::FLUSH_DISTRIBUTED:
        case Type::RESTORE_REPLICA:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ false))
                return false;
            break;
        }

        case Type::STOP_MERGES:
        case Type::START_MERGES:
        {
            String storage_policy_str;
            String volume_str;

            auto parse_on_volume = [&]() -> bool
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

                return true;
            };

            if (ParserKeyword{"ON VOLUME"}.ignore(pos, expected))
            {
                if (!parse_on_volume())
                    return false;
            }
            else
            {
                if (!parseQueryWithOnCluster(res, pos, expected))
                    return false;
                if (ParserKeyword{"ON VOLUME"}.ignore(pos, expected))
                {
                    if (!parse_on_volume())
                        return false;
                }
            }

            res->storage_policy = storage_policy_str;
            res->volume = volume_str;
            if (res->volume.empty() && res->storage_policy.empty())
                parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
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
        case Type::STOP_PULLING_REPLICATION_LOG:
        case Type::START_PULLING_REPLICATION_LOG:
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
            break;

        case Type::SUSPEND:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;

            ASTPtr seconds;
            if (!(ParserKeyword{"FOR"}.ignore(pos, expected)
                && ParserUnsignedInteger().parse(pos, seconds, expected)
                && ParserKeyword{"SECOND"}.ignore(pos, expected)))   /// SECOND, not SECONDS to be consistent with INTERVAL parsing in SQL
            {
                return false;
            }

            res->seconds = seconds->as<ASTLiteral>()->value.get<UInt64>();
            break;
        }
        case Type::DROP_FILESYSTEM_CACHE:
        {
            ParserLiteral path_parser;
            ASTPtr ast;
            if (path_parser.parse(pos, ast, expected))
            {
                res->filesystem_cache_name = ast->as<ASTLiteral>()->value.safeGet<String>();
                if (ParserKeyword{"KEY"}.ignore(pos, expected) && ParserIdentifier().parse(pos, ast, expected))
                {
                    res->key_to_drop = ast->as<ASTIdentifier>()->name();
                    if (ParserKeyword{"OFFSET"}.ignore(pos, expected) && ParserLiteral().parse(pos, ast, expected))
                        res->offset_to_drop = ast->as<ASTLiteral>()->value.safeGet<UInt64>();
                }
            }
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            break;
        }
        case Type::SYNC_FILESYSTEM_CACHE:
        {
            ParserLiteral path_parser;
            ASTPtr ast;
            if (path_parser.parse(pos, ast, expected))
                res->filesystem_cache_name = ast->as<ASTLiteral>()->value.safeGet<String>();
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            break;
        }
        case Type::DROP_SCHEMA_CACHE:
        {
            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                if (ParserKeyword{"FILE"}.ignore(pos, expected))
                    res->schema_cache_storage = "FILE";
                else if (ParserKeyword{"S3"}.ignore(pos, expected))
                    res->schema_cache_storage = "S3";
                else if (ParserKeyword{"HDFS"}.ignore(pos, expected))
                    res->schema_cache_storage = "HDFS";
                else if (ParserKeyword{"URL"}.ignore(pos, expected))
                    res->schema_cache_storage = "URL";
                else if (ParserKeyword{"AZURE"}.ignore(pos, expected))
                    res->schema_cache_storage = "AZURE";
                else
                    return false;
            }
            break;
        }

        case Type::UNFREEZE:
        {
            ASTPtr ast;
            if (ParserKeyword{"WITH NAME"}.ignore(pos, expected) && ParserStringLiteral{}.parse(pos, ast, expected))
            {
                res->backup_name = ast->as<ASTLiteral &>().value.get<const String &>();
            }
            else
            {
                return false;
            }
            break;
        }

        case Type::START_LISTEN:
        case Type::STOP_LISTEN:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;

            auto parse_server_type = [&](ServerType::Type & type, std::string & custom_name) -> bool
            {
                type = ServerType::Type::END;
                custom_name = "";

                for (const auto & cur_type : magic_enum::enum_values<ServerType::Type>())
                {
                    if (ParserKeyword{ServerType::serverTypeToString(cur_type)}.ignore(pos, expected))
                    {
                        type = cur_type;
                        break;
                    }
                }

                if (type == ServerType::Type::END)
                    return false;

                if (type == ServerType::CUSTOM)
                {
                    ASTPtr ast;

                    if (!ParserStringLiteral{}.parse(pos, ast, expected))
                        return false;

                    custom_name = ast->as<ASTLiteral &>().value.get<const String &>();
                }

                return true;
            };

            ServerType::Type base_type;
            std::string base_custom_name;

            ServerType::Types exclude_type;
            ServerType::CustomNames exclude_custom_names;

            if (!parse_server_type(base_type, base_custom_name))
                return false;

            if (ParserKeyword{"EXCEPT"}.ignore(pos, expected))
            {
                if (base_type != ServerType::Type::QUERIES_ALL &&
                    base_type != ServerType::Type::QUERIES_DEFAULT &&
                    base_type != ServerType::Type::QUERIES_CUSTOM)
                    return false;

                ServerType::Type current_type;
                std::string current_custom_name;

                while (true)
                {
                    if (!exclude_type.empty() && !ParserToken(TokenType::Comma).ignore(pos, expected))
                        break;

                    if (!parse_server_type(current_type, current_custom_name))
                        return false;

                    exclude_type.insert(current_type);

                    if (current_type == ServerType::Type::CUSTOM)
                        exclude_custom_names.insert(current_custom_name);
                }
            }

            res->server_type = ServerType(base_type, base_custom_name, exclude_type, exclude_custom_names);

            break;
        }

        default:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            break;
        }
    }

    if (res->database)
        res->children.push_back(res->database);
    if (res->table)
        res->children.push_back(res->table);

    node = std::move(res);
    return true;
}

}
