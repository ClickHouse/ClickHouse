#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Poco/String.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/InstrumentationManager.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperPathUtils.h>

#include <base/EnumReflection.h>

#include <algorithm>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

[[nodiscard]] static bool parseQueryWithOnClusterAndMaybeTable(boost::intrusive_ptr<ASTSystemQuery> & res, IParser::Pos & pos,
                                                 Expected & expected, bool require_table, bool allow_string_literal)
{
    /// Better form for user: SYSTEM <ACTION> table ON CLUSTER cluster
    /// Query rewritten form + form while executing on cluster: SYSTEM <ACTION> ON CLUSTER cluster table
    /// Need to support both
    String cluster;
    bool parsed_on_cluster = false;

    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
        parsed_on_cluster = true;
    }

    bool parsed_table = false;
    bool children_already_added = false;
    if (allow_string_literal)
    {
        ASTPtr ast;
        if (ParserStringLiteral{}.parse(pos, ast, expected))
        {
            String name = ast->as<ASTLiteral &>().value.safeGet<String>();
            /// The string literal may contain 'database.table', split it
            /// to match what parseDatabaseAndTableAsAST would produce.
            auto dot_pos = name.find('.');
            if (dot_pos != String::npos)
            {
                res->setDatabase(name.substr(0, dot_pos));
                res->setTable(name.substr(dot_pos + 1));
            }
            else
            {
                res->setTable(name);
            }
            parsed_table = true;
            children_already_added = true; /// setDatabase/setTable already push to children
        }
    }

    if (!parsed_table)
        parsed_table = parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);

    if (!parsed_table && require_table)
        return false;

    if (!parsed_on_cluster && ParserKeyword{Keyword::ON}.ignore(pos, expected))
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;

    res->cluster = cluster;

    if (!children_already_added)
    {
        if (res->database)
            res->children.push_back(res->database);
        if (res->table)
            res->children.push_back(res->table);
    }

    return true;
}

enum class SystemQueryTargetType : uint8_t
{
    Function,
    Disk,
};

[[nodiscard]] static bool parseQueryWithOnClusterAndTarget(boost::intrusive_ptr<ASTSystemQuery> & res, IParser::Pos & pos, Expected & expected, SystemQueryTargetType target_type)
{
    /// Better form for user: SYSTEM <ACTION> target_name ON CLUSTER cluster
    /// Query rewritten form + form while executing on cluster: SYSTEM <ACTION> ON CLUSTER cluster target_name
    /// Need to support both

    String cluster;
    bool parsed_on_cluster = false;

    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
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

    if (!parsed_on_cluster && ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
    }

    res->cluster = cluster;

    switch (target_type)
    {
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

[[nodiscard]] static bool parseQueryWithOnCluster(boost::intrusive_ptr<ASTSystemQuery> & res, IParser::Pos & pos,
                                    Expected & expected)
{
    String cluster_str;
    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }
    res->cluster = cluster_str;

    return true;
}

[[nodiscard]] static bool parseDropReplica(boost::intrusive_ptr<ASTSystemQuery> & res, IParser::Pos & pos, Expected & expected, bool database)
{
    if (!parseQueryWithOnCluster(res, pos, expected))
        return false;

    ASTPtr ast;
    if (!ParserStringLiteral{}.parse(pos, ast, expected))
        return false;
    res->replica = ast->as<ASTLiteral &>().value.safeGet<String>();

    if (ParserKeyword{Keyword::FROM_SHARD}.ignore(pos, expected))
    {
        if (!ParserStringLiteral{}.parse(pos, ast, expected))
            return false;
        res->shard = ast->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (ParserKeyword{Keyword::FROM}.ignore(pos, expected))
    {
        // way 1. parse replica database
        // way 2. parse replica table
        // way 3. parse replica zkpath
        if (ParserKeyword{Keyword::DATABASE}.ignore(pos, expected))
        {
            ParserIdentifier database_parser;
            if (!database_parser.parse(pos, res->database, expected))
                return false;
        }
        else if (!database && ParserKeyword{Keyword::TABLE}.ignore(pos, expected))
        {
            parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
        }
        else if (ParserKeyword{Keyword::ZKPATH}.ignore(pos, expected))
        {
            ASTPtr path_ast;
            if (!ParserStringLiteral{}.parse(pos, path_ast, expected))
                return false;
            String zk_path = path_ast->as<ASTLiteral &>().value.safeGet<String>();

            /// Reject empty/root-only keeper paths at parse time (see #109217). For "", "/", "//",
            /// "aux:/", "aux://" the fully collapsed keeper path is empty/root, so the malformed query
            /// is caught here instead of building a lossy AST that fails the debug round-trip self-check.
            /// (Short-circuit: for an empty literal the helper would throw a less specific message.)
            if (zk_path.empty()
                || zkutil::extractZooKeeperPathAndCollapseTrailingSlashes(zk_path, /*check_starts_with_slash*/ false)
                       .find_first_not_of('/') == String::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path in DROP REPLICA is empty or refers to the root");

            res->zk_name = zkutil::extractZooKeeperName(zk_path);

            /// Store the drop path with the legacy one-slash normalization (strip one trailing slash
            /// here, extractZooKeeperPath strips one more) so it matches how tables and Replicated
            /// databases store their keeper path (TableZnodeInfo::resolve / DatabaseReplicated). Fully
            /// collapsing here would make a remote-replica drop of an object created with a trailing
            /// slash probe the wrong znode. The self-protection guards collapse both sides instead.
            String legacy_path = zk_path;
            if (legacy_path.back() == '/')
                legacy_path.pop_back();
            res->replica_zk_path = zkutil::extractZooKeeperPath(legacy_path, /*check_starts_with_slash*/ false);

            /// Keep the raw literal for formatting so the AST round-trips losslessly (formatImpl prints
            /// full_replica_zk_path; the interpreter uses the normalized replica_zk_path for the drop).
            res->full_replica_zk_path = std::move(zk_path);
        }
        else
            return false;

        if (database && ParserKeyword{Keyword::WITH_TABLES}.ignore(pos, expected))
            res->with_tables = true;
    }
    else
        res->is_drop_whole_replica = true;

    return true;
}

[[nodiscard]] static bool parseDropCatalogReplica(boost::intrusive_ptr<ASTSystemQuery> & res, IParser::Pos & pos, Expected & expected)
{
    ASTPtr ast;
    if (!ParserStringLiteral{}.parse(pos, ast, expected))
        return false;
    res->replica = ast->as<ASTLiteral &>().value.safeGet<String>();
    return true;
}

bool ParserSystemQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::SYSTEM}.ignore(pos, expected))
        return false;

    using Type = ASTSystemQuery::Type;

    auto res = make_intrusive<ASTSystemQuery>();

    bool found = false;

    static constexpr Type background_verbs[] = {
        Type::STOP,
        Type::START,
        Type::PAUSE,
        Type::CANCEL,
        Type::REFRESH,
    };

    for (const auto & type : magic_enum::enum_values<Type>())
    {
        /// STOP matches also STOP [...], check the more specific forms first.
        if (std::ranges::contains(background_verbs, type))
            continue;
        if (ParserKeyword::createDeprecated(ASTSystemQuery::typeToString(type)).ignore(pos, expected))
        {
            res->type = type;
            found = true;
            break;
        }
    }

    /// SYSTEM DROP [...] CACHE sounds like the statement disables the cache but it merely clears it.
    /// SYSTEM CLEAR [...] CACHE is the preferred syntax but we need to retain support for DROP.
    if (!found)
    {
        static const std::vector<std::pair<std::string_view, Type>> system_aliases = {
            {"DROP DNS CACHE", Type::CLEAR_DNS_CACHE},
            {"DROP CONNECTIONS CACHE", Type::CLEAR_CONNECTIONS_CACHE},
            {"DROP MARK CACHE", Type::CLEAR_MARK_CACHE},
            {"DROP PRIMARY INDEX CACHE", Type::CLEAR_PRIMARY_INDEX_CACHE},
            {"DROP UNCOMPRESSED CACHE", Type::CLEAR_UNCOMPRESSED_CACHE},
            {"DROP INDEX MARK CACHE", Type::CLEAR_INDEX_MARK_CACHE},
            {"DROP INDEX UNCOMPRESSED CACHE", Type::CLEAR_INDEX_UNCOMPRESSED_CACHE},
            {"DROP VECTOR SIMILARITY INDEX CACHE", Type::CLEAR_VECTOR_SIMILARITY_INDEX_CACHE},
            {"DROP TEXT INDEX TOKENS CACHE", Type::CLEAR_TEXT_INDEX_TOKENS_CACHE},
            {"DROP TEXT INDEX HEADER CACHE", Type::CLEAR_TEXT_INDEX_HEADER_CACHE},
            {"DROP TEXT INDEX POSTINGS CACHE", Type::CLEAR_TEXT_INDEX_POSTINGS_CACHE},
            {"DROP TEXT INDEX CACHES", Type::CLEAR_TEXT_INDEX_CACHES},
            {"DROP MMAP CACHE", Type::CLEAR_MMAP_CACHE},
            {"DROP QUERY CONDITION CACHE", Type::CLEAR_QUERY_CONDITION_CACHE},
            {"DROP ENCRYPTION HEADERS CACHE", Type::CLEAR_ENCRYPTION_HEADERS_CACHE},
            {"DROP QUERY CACHE", Type::CLEAR_QUERY_CACHE},
            {"DROP COMPILED EXPRESSION CACHE", Type::CLEAR_COMPILED_EXPRESSION_CACHE},
            {"DROP ICEBERG METADATA CACHE", Type::CLEAR_ICEBERG_METADATA_CACHE},
            {"DROP PAIMON METADATA CACHE", Type::CLEAR_PAIMON_METADATA_CACHE},
            {"DROP PARQUET METADATA CACHE", Type::CLEAR_PARQUET_METADATA_CACHE},
            {"DROP POINT IN POLYGON CACHE", Type::CLEAR_POINT_IN_POLYGON_CACHE},
            {"DROP FILESYSTEM CACHE", Type::CLEAR_FILESYSTEM_CACHE},
            {"DROP DISTRIBUTED CACHE", Type::CLEAR_DISTRIBUTED_CACHE},
            {"DROP DISK METADATA CACHE", Type::CLEAR_DISK_METADATA_CACHE},
            {"DROP PAGE CACHE", Type::CLEAR_PAGE_CACHE},
            {"DROP SCHEMA CACHE", Type::CLEAR_SCHEMA_CACHE},
            {"DROP FORMAT SCHEMA CACHE", Type::CLEAR_FORMAT_SCHEMA_CACHE},
            {"DROP AVRO SCHEMA CACHE", Type::CLEAR_AVRO_SCHEMA_CACHE},
            {"DROP S3 CLIENT CACHE", Type::CLEAR_S3_CLIENT_CACHE},
        };

        for (const auto & [alias, type] : system_aliases)
        {
            if (ParserKeyword::createDeprecatedPtr(alias)->ignore(pos, expected))
            {
                res->type = type;
                found = true;
                break;
            }
        }
    }

    if (!found)
    {
        for (const auto & type : background_verbs)
        {
            if (ParserKeyword::createDeprecated(ASTSystemQuery::typeToString(type)).ignore(pos, expected))
            {
                res->type = type;
                found = true;
                break;
            }
        }
    }

    if (!found)
        return false;

    if (res->type == Type::UNKNOWN || res->type == Type::END)
        return false;


    switch (res->type)
    {
        case Type::RELOAD_DICTIONARY:
        case Type::UNLOAD_DICTIONARY: {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ true))
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
        case Type::DROP_CATALOG_REPLICA:
        {
            if (!parseDropCatalogReplica(res, pos, expected))
                return false;
            break;
        }
        case Type::ALLOCATE_MEMORY:
        {
            ASTPtr ast;
            if (!ParserUnsignedInteger().parse(pos, ast, expected))
                return false;

            res->untracked_memory_size = ast->as<ASTLiteral &>().value.safeGet<UInt64>();
            break;
        }
        case Type::ENABLE_FAILPOINT:
        case Type::DISABLE_FAILPOINT:
        case Type::NOTIFY_FAILPOINT:
        {
            ASTPtr ast;
            if (ParserIdentifier{}.parse(pos, ast, expected))
                res->fail_point_name = ast->as<ASTIdentifier &>().name();
            else
                return false;
            break;
        }
        case Type::WAIT_FAILPOINT:
        {
            ASTPtr ast;
            if (ParserIdentifier{}.parse(pos, ast, expected))
                res->fail_point_name = ast->as<ASTIdentifier &>().name();
            else
                return false;

            /// Optional PAUSE or RESUME keyword
            if (ParserKeyword(Keyword::PAUSE).ignore(pos, expected))
                res->fail_point_action = ASTSystemQuery::FailPointAction::PAUSE;
            else if (ParserKeyword(Keyword::RESUME).ignore(pos, expected))
                res->fail_point_action = ASTSystemQuery::FailPointAction::RESUME;

            break;
        }
        case Type::RELOAD_DELTA_KERNEL_TRACING:
        {
            ASTPtr ast;
            if (ParserIdentifier{}.parse(pos, ast, expected))
                res->delta_kernel_tracing_level = ast->as<ASTIdentifier &>().name();
            else
                return false;
            break;
        }
        case Type::SET_COVERAGE_TEST:
        {
            ASTPtr ast;
            if (ParserStringLiteral{}.parse(pos, ast, expected))
                res->coverage_test_name = ast->as<ASTLiteral &>().value.safeGet<String>();
            break;
        }

        case Type::RESTART_REPLICA:
        case Type::SYNC_REPLICA:
        case Type::WAIT_LOADING_PARTS:
        case Type::WAIT_QUERY_RUNNER:
        case Type::PREWARM_MARK_CACHE:
        case Type::PREWARM_PRIMARY_INDEX_CACHE:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;
            if (res->type == Type::SYNC_REPLICA)
            {
                if (ParserKeyword{Keyword::IF_EXISTS}.ignore(pos, expected))
                    res->if_exists = true;

                if (ParserKeyword{Keyword::STRICT}.ignore(pos, expected))
                    res->sync_replica_mode = SyncReplicaMode::STRICT;
                else if (ParserKeyword{Keyword::LIGHTWEIGHT}.ignore(pos, expected))
                {
                    res->sync_replica_mode = SyncReplicaMode::LIGHTWEIGHT;
                    if (ParserKeyword{Keyword::FROM}.ignore(pos, expected))
                    {
                        do
                        {
                            ASTPtr replica_ast;
                            if (!ParserStringLiteral{}.parse(pos, replica_ast, expected))
                                return false;
                            res->src_replicas.emplace_back(replica_ast->as<ASTLiteral &>().value.safeGet<String>());
                        } while (ParserToken{TokenType::Comma}.ignore(pos, expected));
                    }
                }
                else if (ParserKeyword{Keyword::PULL}.ignore(pos, expected))
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
            if (ParserKeyword{Keyword::STRICT}.ignore(pos, expected))
                res->sync_replica_mode = SyncReplicaMode::STRICT;
            break;
        }
        case Type::RESTART_DISK:
        case Type::CLEAR_DISK_METADATA_CACHE:
        case Type::WAIT_BLOBS_CLEANUP:
        {
            if (!parseQueryWithOnClusterAndTarget(res, pos, expected, SystemQueryTargetType::Disk))
                return false;
            break;
        }
        /// FLUSH DISTRIBUTED requires table
        /// START/STOP DISTRIBUTED SENDS does not require table
        case Type::STOP_DISTRIBUTED_SENDS:
        case Type::START_DISTRIBUTED_SENDS:
        case Type::LOAD_PRIMARY_KEY:
        case Type::UNLOAD_PRIMARY_KEY:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ false, /* allow_string_literal = */ false))
                return false;
            break;
        }

        case Type::FLUSH_OBJECT_STORAGE_QUEUE:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ false))
                return false;
            if (!ParserKeyword{Keyword::PATH}.ignore(pos, expected))
                return false;
            ASTPtr path_ast;
            if (!ParserStringLiteral{}.parse(pos, path_ast, expected))
                return false;
            res->queue_path = path_ast->as<ASTLiteral &>().value.safeGet<String>();
            break;
        }

        case Type::FLUSH_DISTRIBUTED:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ false))
                return false;

            ParserKeyword s_settings(Keyword::SETTINGS);
            if (s_settings.ignore(pos, expected))
            {
                ParserSetQuery parser_settings(/* parse_only_internals_= */ true);
                if (!parser_settings.parse(pos, res->query_settings, expected))
                    return false;
            }

            break;
        }

        case Type::RESTORE_REPLICA:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ false))
                return false;
            break;
        }
        case Type::RESTORE_DATABASE_REPLICA:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            if (!parseDatabaseAsAST(pos, expected, res->database))
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

            if (ParserKeyword{Keyword::ON_VOLUME}.ignore(pos, expected))
            {
                if (!parse_on_volume())
                    return false;
            }
            else
            {
                if (!parseQueryWithOnCluster(res, pos, expected))
                    return false;
                if (ParserKeyword{Keyword::ON_VOLUME}.ignore(pos, expected))
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
        case Type::STOP_CLEANUP:
        case Type::START_CLEANUP:
        case Type::STOP_VIRTUAL_PARTS_UPDATE:
        case Type::START_VIRTUAL_PARTS_UPDATE:
        case Type::STOP_REDUCE_BLOCKING_PARTS:
        case Type::START_REDUCE_BLOCKING_PARTS:
        case Type::SYNC_MERGES:
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            parseDatabaseAndTableAsAST(pos, expected, res->database, res->table);
            break;

        case Type::SCHEDULE_MERGE:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;
            if (!ParserKeyword{Keyword::PARTS}.ignore(pos, expected))
                return false;
            ParserList parser_list(std::make_unique<ParserStringLiteral>(),
                                   std::make_unique<ParserToken>(TokenType::Comma),
                                   /*allow_empty=*/false);
            ASTPtr parts;
            if (!parser_list.parse(pos, parts, expected))
                return false;
            res->scheduled_merge_parts = parts;
            res->children.push_back(parts);
            break;
        }

        case Type::REFRESH_VIEW:
        case Type::WAIT_VIEW:
        case Type::START_VIEW:
        case Type::START_REPLICATED_VIEW:
        case Type::STOP_VIEW:
        case Type::STOP_REPLICATED_VIEW:
        case Type::PAUSE_VIEW:
        case Type::CANCEL_VIEW:
        case Type::STOP:
        case Type::START:
        case Type::PAUSE:
        case Type::CANCEL:
        case Type::REFRESH:
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;
            break;

        case Type::START_VIEWS:
        case Type::STOP_VIEWS:
        case Type::PAUSE_VIEWS:
        case Type::FREE_MEMORY:
        case Type::STOP_ALL_BACKGROUND:
        case Type::START_ALL_BACKGROUND:
        case Type::PAUSE_ALL_BACKGROUND:
        case Type::CANCEL_ALL_BACKGROUND:
        case Type::REFRESH_ALL_BACKGROUND:
            break;

        case Type::TEST_VIEW:
        {
            if (!parseDatabaseAndTableAsAST(pos, expected, res->database, res->table))
                return false;

            if (ParserKeyword{Keyword::UNSET_FAKE_TIME}.ignore(pos, expected))
                break;

            if (!ParserKeyword{Keyword::SET_FAKE_TIME}.ignore(pos, expected))
                return false;
            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            res->fake_time_for_view = ast->as<ASTLiteral &>().value.safeGet<String>();

            break;
        }

        case Type::SUSPEND:
        {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;

            ASTPtr seconds;
            if (!(ParserKeyword{Keyword::FOR}.ignore(pos, expected)
                && ParserUnsignedInteger().parse(pos, seconds, expected)
                && ParserKeyword{Keyword::SECOND}.ignore(pos, expected)))   /// SECOND, not SECONDS to be consistent with INTERVAL parsing in SQL
            {
                return false;
            }

            res->seconds = seconds->as<ASTLiteral>()->value.safeGet<UInt64>();
            break;
        }
        case Type::CLEAR_QUERY_CACHE:
        {
            ParserLiteral tag_parser;
            ASTPtr ast;
            if (ParserKeyword{Keyword::TAG}.ignore(pos, expected) && tag_parser.parse(pos, ast, expected))
                res->query_result_cache_tag = std::make_optional<String>(ast->as<ASTLiteral>()->value.safeGet<String>());
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            break;
        }
        case Type::CLEAR_FILESYSTEM_CACHE:
        {
            ParserLiteral path_parser;
            ASTPtr ast;
            if (path_parser.parse(pos, ast, expected))
            {
                res->filesystem_cache_name = ast->as<ASTLiteral>()->value.safeGet<String>();
                if (ParserKeyword{Keyword::KEY}.ignore(pos, expected) && ParserIdentifier().parse(pos, ast, expected))
                {
                    res->key_to_drop = ast->as<ASTIdentifier>()->name();
                    if (ParserKeyword{Keyword::OFFSET}.ignore(pos, expected) && ParserLiteral().parse(pos, ast, expected))
                        res->offset_to_drop = ast->as<ASTLiteral>()->value.safeGet<UInt64>();
                }
            }
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
            break;
        }
        case Type::CLEAR_DISTRIBUTED_CACHE:
        {
            ParserLiteral parser;
            ASTPtr ast;
            if (ParserKeyword{Keyword::CONNECTIONS}.ignore(pos, expected))
            {
                res->distributed_cache_drop_connections = true;
            }
            else if (parser.parse(pos, ast, expected))
            {
                res->distributed_cache_server_id = ast->as<ASTLiteral>()->value.safeGet<String>();
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
        case Type::CLEAR_SCHEMA_CACHE:
        {
            if (ParserKeyword{Keyword::FOR}.ignore(pos, expected))
            {
                if (ParserKeyword{Keyword::FILE}.ignore(pos, expected))
                    res->schema_cache_storage = toStringView(Keyword::FILE);
                else if (ParserKeyword{Keyword::S3}.ignore(pos, expected))
                    res->schema_cache_storage = toStringView(Keyword::S3);
                else if (ParserKeyword{Keyword::HDFS}.ignore(pos, expected))
                    res->schema_cache_storage = toStringView(Keyword::HDFS);
                else if (ParserKeyword{Keyword::URL}.ignore(pos, expected))
                    res->schema_cache_storage = toStringView(Keyword::URL);
                else if (ParserKeyword{Keyword::AZURE}.ignore(pos, expected))
                    res->schema_cache_storage = toStringView(Keyword::AZURE);
                else
                    return false;
            }
            break;
        }
        case Type::CLEAR_FORMAT_SCHEMA_CACHE:
        {
            if (ParserKeyword{Keyword::FOR}.ignore(pos, expected))
            {
                if (ParserKeyword{Keyword::PROTOBUF}.ignore(pos, expected))
                    res->schema_cache_format = toStringView(Keyword::PROTOBUF);
                else if (ParserKeyword{Keyword::FILES}.ignore(pos, expected))
                    res->schema_cache_format = toStringView(Keyword::FILES);
                else
                    return false;
            }
            break;
        }
        case Type::UNFREEZE:
        {
            ASTPtr ast;
            if (ParserKeyword{Keyword::WITH_NAME}.ignore(pos, expected) && ParserStringLiteral{}.parse(pos, ast, expected))
            {
                res->backup_name = ast->as<ASTLiteral &>().value.safeGet<String>();
            }
            else
            {
                return false;
            }
            break;
        }
        case Type::UNLOCK_SNAPSHOT:
        {

            ASTPtr ast;
            if (ParserStringLiteral{}.parse(pos, ast, expected))
            {
                res->backup_name = ast->as<ASTLiteral &>().value.safeGet<String>();
            }
            else
                return false;

            if (ParserKeyword{Keyword::FROM}.ignore(pos, expected) && ParserIdentifierWithOptionalParameters{}.parse(pos, ast, expected))
            {
                ast->as<ASTFunction &>().setKind(ASTFunction::Kind::BACKUP_NAME);
                res->backup_source = ast;
                res->children.push_back(res->backup_source);
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
                    if (ParserKeyword::createDeprecated(ServerType::serverTypeToString(cur_type)).ignore(pos, expected))
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

                    custom_name = ast->as<ASTLiteral &>().value.safeGet<String>();
                }

                return true;
            };

            ServerType::Type base_type = {};
            std::string base_custom_name;

            ServerType::Types exclude_type;
            ServerType::CustomNames exclude_custom_names;

            if (!parse_server_type(base_type, base_custom_name))
                return false;

            if (ParserKeyword{Keyword::EXCEPT}.ignore(pos, expected))
            {
                if (base_type != ServerType::Type::QUERIES_ALL &&
                    base_type != ServerType::Type::QUERIES_DEFAULT &&
                    base_type != ServerType::Type::QUERIES_CUSTOM)
                    return false;

                ServerType::Type current_type = {};
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

        case Type::FLUSH_ASYNC_INSERT_QUEUE:
        case Type::FLUSH_LOGS:
        {
            Pos prev_token = pos;
            if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
            {
                pos = prev_token;
                if (!parseQueryWithOnCluster(res, pos, expected))
                    return false;
            }

            ParserToken s_dot(TokenType::Dot);
            ParserIdentifier table_parser(true);

            do
            {
                ASTPtr table_first;
                if (!table_parser.parse(pos, table_first, expected))
                {
                    if (res->tables.empty())
                        break;
                    return false;
                }

                if (!s_dot.ignore(pos))
                {
                    res->tables.emplace_back(String{}, table_first->as<ASTIdentifier &>().full_name);
                }
                else
                {
                    ASTPtr table_second;
                    if (!table_parser.parse(pos, table_second, expected))
                        return false;
                    res->tables.emplace_back(table_first->as<ASTIdentifier &>().full_name, table_second->as<ASTIdentifier &>().full_name);
                }


            } while (ParserToken{TokenType::Comma}.ignore(pos, expected));

            break;
        }

#if USE_XRAY
        case Type::INSTRUMENT_REMOVE:
        {
            ASTPtr temporary_identifier;

            if (ParserSubquery{}.parse(pos, temporary_identifier, expected))
            {
                if (!temporary_identifier->children.empty())
                {
                    WriteBufferFromOwnString query_buffer;
                    IAST::FormatSettings settings(true);
                    temporary_identifier->children[0]->format(query_buffer, settings);
                    res->instrumentation_subquery = query_buffer.str();
                }
                break;
            }

            if (ParserLiteral{}.parse(pos, temporary_identifier, expected))
            {
                const auto field = temporary_identifier->as<ASTLiteral &>().value;
                switch (field.getType())
                {
                    case Field::Types::Which::String:
                        res->instrumentation_point = field.safeGet<String>();
                        break;
                    case Field::Types::Which::UInt64:
                        res->instrumentation_point = field.safeGet<UInt64>();
                        break;
                    default:
                        expected.add(pos, "String or UInt64 literal for instrumentation point");
                        return false;
                }
            }
            else if (ParserIdentifier{}.parse(pos, temporary_identifier, expected))
            {
                String identifier = temporary_identifier->as<ASTIdentifier &>().name();
                if (Poco::toLower(identifier) == "all")
                    res->instrumentation_point = Instrumentation::All{};
                else
                {
                    expected.add(pos, "ALL");
                    return false;
                }
            }
            else
            {
                expected.add(pos, "instrumentation point: subquery, literal, or ALL");
                return false;
            }

            break;
        }
        case Type::INSTRUMENT_ADD:
        {
            ASTPtr temporary_identifier;
            if (ParserLiteral{}.parse(pos, temporary_identifier, expected))
                res->instrumentation_function_name = temporary_identifier->as<ASTLiteral &>().value.safeGet<String>();
            else
            {
                expected.add(pos, "function name (string literal)");
                return false;
            }

            if (ParserIdentifier{}.parse(pos, temporary_identifier, expected))
                res->instrumentation_handler_name = temporary_identifier->as<ASTIdentifier &>().name();
            else
            {
                expected.add(pos, "handler name (LOG, SLEEP, or PROFILE)");
                return false;
            }

            if (Poco::toLower(res->instrumentation_handler_name) == "profile")
            {
                res->instrumentation_entry_type = Instrumentation::EntryType::ENTRY_AND_EXIT;
                break;
            }

            if (ParserIdentifier{}.parse(pos, temporary_identifier, expected))
            {
                String entry_type = temporary_identifier->as<ASTIdentifier &>().name();
                if (Poco::toLower(entry_type) == "entry")
                    res->instrumentation_entry_type = Instrumentation::EntryType::ENTRY;
                else if (Poco::toLower(entry_type) == "exit")
                    res->instrumentation_entry_type = Instrumentation::EntryType::EXIT;
                else
                {
                    expected.add(pos, "entry type (ENTRY or EXIT)");
                    return false;
                }
            }
            else
            {
                expected.add(pos, "entry type (ENTRY or EXIT)");
                return false;
            }


            ASTPtr arg_ast;
            while (ParserLiteral{}.parse(pos, arg_ast, expected))
            {
                const auto & value = arg_ast->as<ASTLiteral &>().value;
                if (value.getType() == Field::Types::String)
                    res->instrumentation_arguments.emplace_back(value.safeGet<String>());
                else if (value.getType() == Field::Types::Int64)
                    res->instrumentation_arguments.emplace_back(value.safeGet<Int64>());
                else if (value.getType() == Field::Types::UInt64)
                {
                    UInt64 uint_value = value.safeGet<UInt64>();
                    if (uint_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                    {
                        expected.add(pos, "integer literal not exceeding Int64 maximum");
                        return false;
                    }
                    res->instrumentation_arguments.emplace_back(static_cast<Int64>(uint_value));
                }
                else if (value.getType() == Field::Types::Float64)
                    res->instrumentation_arguments.emplace_back(value.safeGet<Float64>());
                else
                {
                    expected.add(pos, "string, integer, or float literal argument");
                    return false;
                }
            }

            if (res->instrumentation_arguments.empty())
            {
                expected.add(pos, "at least one argument (string, integer, or float literal)");
                return false;
            }

            break;
        }
#endif

#if USE_JEMALLOC
        case Type::JEMALLOC_FLUSH_PROFILE:
        {
            Pos prev_token = pos;
            if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
            {
                pos = prev_token;
                if (!parseQueryWithOnCluster(res, pos, expected))
                    return false;
            }
            break;
        }
#endif
        case Type::RESET_DDL_WORKER: {
            if (!parseQueryWithOnCluster(res, pos, expected))
                return false;
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
    if (res->query_settings)
        res->children.push_back(res->query_settings);

    node = std::move(res);
    return true;
}

}

namespace DB
{

void registerStatementSystem(StatementFactory & factory)
{
    factory.registerStatement("SYSTEM",
    {
        .description = R"DOCS_MD(
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

## SYSTEM RELOAD EMBEDDED DICTIONARIES {#reload-embedded-dictionaries}

Reload all [Internal dictionaries](/reference/statements/create/dictionary).
By default, internal dictionaries are disabled.
Always returns `Ok.` regardless of the result of the internal dictionary update.

## SYSTEM RELOAD DICTIONARIES {#reload-dictionaries}

The `SYSTEM RELOAD DICTIONARIES` query reloads dictionaries with a status of `LOADED` (see the `status` column of [`system.dictionaries`](/reference/system-tables/dictionaries)), i.e dictionaries that have been successfully loaded before.
By default, dictionaries are loaded lazily (see [dictionaries_lazy_load](/reference/settings/server-settings/settings/dictionaries#dictionaries_lazy_load)), so instead of being loaded automatically at startup, they are initialized on first access through use of the [`dictGet`](/reference/functions/regular-functions/ext-dict-functions#dictGet) function or use of `SELECT` from tables with `ENGINE = Dictionary`.

**Syntax**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

## SYSTEM RELOAD DICTIONARY {#reload-dictionary}

Completely reloads a dictionary `dictionary_name`, regardless of the state of the dictionary (LOADED / NOT_LOADED / FAILED).
Always returns `Ok.` regardless of the result of updating the dictionary.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

The status of the dictionary can be checked by querying the `system.dictionaries` table.

```sql
SELECT name, status FROM system.dictionaries;
```

## SYSTEM UNLOAD DICTIONARY {#unload-dictionary}

Unloads a dictionary `dictionary_name` to release its memory, if the dictionary status is `LOADED`.
The dictionary is lazy-reloaded when necessary again.

```sql
SYSTEM UNLOAD DICTIONARY dictionary_name
```

The status of the dictionary can be checked by querying the `system.dictionaries` table.

```sql
SELECT name, status FROM system.dictionaries;
```

## SYSTEM UNLOAD DICTIONARIES {#unload-dictionaries}

The `SYSTEM UNLOAD DICTIONARIES` query unloads all dictionaries with a `LOADED` status (see the `status` column of [`system.dictionaries`](/reference/system-tables/dictionaries)), i.e dictionaries that have been successfully loaded before.

```sql
SYSTEM UNLOAD DICTIONARIES
```

## SYSTEM RELOAD FUNCTIONS {#reload-functions}

Reloads all registered [executable user defined functions](/reference/functions/regular-functions/udf#executable-user-defined-functions) or one of them from a configuration file.

**Syntax**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

## SYSTEM RELOAD ASYNCHRONOUS METRICS {#reload-asynchronous-metrics}

Re-calculates all [asynchronous metrics](/reference/system-tables/asynchronous_metrics). Since asynchronous metrics are periodically updated based on setting [asynchronous_metrics_update_period_s](/reference/settings/server-settings/settings/asynchronous-metrics#asynchronous_metrics_update_period_s), updating them manually using this statement is typically not necessary.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

## SYSTEM CLEAR|DROP DNS CACHE {#drop-dns-cache}

Clears ClickHouse's internal DNS cache. Sometimes (for old ClickHouse versions) it is necessary to use this command when changing the infrastructure (changing the IP address of another ClickHouse server or the server used by dictionaries).

For more convenient (automatic) cache management, see `disable_internal_dns_cache`, `dns_cache_max_entries`, `dns_cache_update_period` parameters.

## SYSTEM CLEAR|DROP MARK CACHE {#drop-mark-cache}

Clears the mark cache.

## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE {#drop-primary-index-cache}

Clears the primary index cache, which holds the primary keys of [`MergeTree`](/reference/engines/table-engines/mergetree-family/mergetree) tables in memory.
Its size is configured with the server-level setting [`primary_index_cache_size`](/reference/settings/server-settings/settings/primary-index#primary_index_cache_size).

## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE {#drop-iceberg-metadata-cache}

Clears the iceberg metadata cache.

## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE {#drop-avro-schema-cache}

Clears the per-URL Confluent Schema Registry caches used by the `AvroConfluent` format. This drops both the schema-fetch cache (id → schema) and the schema-registration cache (subject + schema → id), so subsequent reads and writes fall back to the registry server. Useful when a schema was deleted or rewritten on the registry side, or to verify the registry's idempotency in tests.

## SYSTEM DROP PARQUET METADATA CACHE {#drop-parquet-metadata-cache}

Clears the parquet metadata cache.

## SYSTEM CLEAR|DROP PAIMON METADATA CACHE {#drop-paimon-metadata-cache}

Clears the in-memory cache of parsed Paimon metadata files (manifest lists and manifests).
## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE {#drop-point-in-polygon-cache}

Clears the cache of preprocessed constant polygons used by the function [`pointInPolygon`](/reference/functions/regular-functions/geo/coordinates#pointinpolygon). The configured size limit (the `point_in_polygon_cache_size` server setting) is left unchanged, so the cache keeps accepting entries afterwards. To disable the cache instead, set `point_in_polygon_cache_size` to `0`.

## SYSTEM CLEAR|DROP TEXT INDEX CACHES {#drop-text-index-caches}

Clears the text index's tokens, header and postings caches.

If you like to drop one of these caches individually, you can run

- `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
- `SYSTEM CLEAR TEXT INDEX HEADER CACHE`, or
- `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

## SYSTEM CLEAR|DROP INDEX MARK CACHE {#drop-index-mark-cache}

Clears the cache of marks for secondary (data-skipping) indexes.

## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE {#drop-index-uncompressed-cache}

Clears the cache of uncompressed blocks for secondary (data-skipping) indexes.

## SYSTEM CLEAR|DROP MMAP CACHE {#drop-mmap-cache}

Clears the cache of memory-mapped files.

## SYSTEM CLEAR|DROP PAGE CACHE {#drop-page-cache}

Clears the userspace page cache, ClickHouse's own in-memory cache of data read from the underlying storage.

## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE {#drop-vector-similarity-index-cache}

Clears the vector similarity index cache.

## SYSTEM CLEAR|DROP CONNECTIONS CACHE {#drop-connections-cache}

Clears the cache of HTTP connection pools used for outgoing connections.

## SYSTEM CLEAR|DROP S3 CLIENT CACHE {#drop-s3-client-cache}

Clears the cache of S3 clients.

## SYSTEM PREWARM MARK CACHE {#prewarm-mark-cache}

Loads the marks of a table into the [mark cache](#drop-mark-cache). Secondary-index marks are also loaded into the [index mark cache](#drop-index-mark-cache).

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

## SYSTEM PREWARM PRIMARY INDEX CACHE {#prewarm-primary-index-cache}

Loads the primary indexes of a `MergeTree` table into the [primary index cache](#drop-primary-index-cache).

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

## SYSTEM CLEAR|DROP DISK METADATA CACHE {#drop-disk-metadata-cache}

Clears the metadata cache of the specified disk.

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

## SYSTEM SYNC FILESYSTEM CACHE {#sync-filesystem-cache}

Reconciles ClickHouse's in-memory state of the filesystem cache with the cache files actually present on disk, and returns the `cache_name`, `path` and downloaded `size` of each cached file segment. An optional cache name limits the operation to a single cache.

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

## SYSTEM CLEAR|DROP DISTRIBUTED CACHE {#drop-distributed-cache}

<Note>
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE` is available only in ClickHouse Cloud.
</Note>

Drops the distributed cache. Use `CONNECTIONS` to drop only the cached connections to the distributed cache servers, or pass a server identifier to target a single server.

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

## SYSTEM DROP REPLICA {#drop-replica}

Dead replicas of `ReplicatedMergeTree` tables can be dropped using following syntax:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

Queries will remove the `ReplicatedMergeTree` replica path in ZooKeeper. It is useful when the replica is dead and its metadata cannot be removed from ZooKeeper by `DROP TABLE` because there is no such table anymore. It will only drop the inactive/stale replica, and it cannot drop local replica, please use `DROP TABLE` for that. `DROP REPLICA` does not drop any tables and does not remove any data or metadata from disk.

The first one removes metadata of `'replica_name'` replica of `database.table` table.
The second one does the same for all replicated tables in the database.
The third one does the same for all replicated tables on the local server.
The fourth one is useful to remove metadata of dead replica when all other replicas of a table were dropped. It requires the table path to be specified explicitly. It must be the same path as was passed to the first argument of `ReplicatedMergeTree` engine on table creation.

## SYSTEM DROP DATABASE REPLICA {#drop-database-replica}

Dead replicas of `Replicated` databases can be dropped using following syntax:

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

Similar to `SYSTEM DROP REPLICA`, but removes the `Replicated` database replica path from ZooKeeper when there's no database to run `DROP DATABASE`. Please note that it does not remove `ReplicatedMergeTree` replicas (so you may need `SYSTEM DROP REPLICA` as well). Shard and replica names are the names that were specified in `Replicated` engine arguments when creating the database. Also, these names can be obtained from `database_shard_name` and `database_replica_name` columns in `system.clusters`. If the `FROM SHARD` clause is missing, then `replica_name` must be a full replica name in `shard_name|replica_name` format.

## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE {#drop-uncompressed-cache}

Clears the uncompressed data cache.
The uncompressed data cache is enabled/disabled with the query/user/profile-level setting [`use_uncompressed_cache`](/reference/settings/session-settings/use#use_uncompressed_cache).
Its size can be configured using the server-level setting [`uncompressed_cache_size`](/reference/settings/server-settings/settings/uncompressed-cache#uncompressed_cache_size).

## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE {#drop-compiled-expression-cache}

Clears the compiled expression cache.
The compiled expression cache is enabled/disabled with the query/user/profile-level setting [`compile_expressions`](/reference/settings/session-settings/compile#compile_expressions).

## SYSTEM CLEAR|DROP QUERY CONDITION CACHE {#drop-query-condition-cache}

Clears the query condition cache.

## SYSTEM CLEAR|DROP ENCRYPTION HEADERS CACHE {#drop-encryption-headers-cache}

Clears the encryption headers cache. This cache holds the encryption headers read from the front of encrypted files and is used by the experimental `use_reader_executor` read path to avoid re-reading them; its size is configured by the `encryption_header_cache_size` server setting.

## SYSTEM CLEAR|DROP QUERY CACHE {#drop-query-cache}

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
````

Clears the [query cache](/concepts/features/performance/caches/query-cache).
If a tag is specified, only query cache entries with the specified tag are deleted.

## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE {#system-drop-schema-format}

Clears cache for schemas loaded from [`format_schema_path`](/reference/settings/server-settings/settings/format#format_schema_path).

Supported targets:
- Protobuf: Removes imported Protobuf message definitions from memory.
- Files: Deletes cached schema files stored locally in the [`format_schema_path`](/reference/settings/server-settings/settings/format#format_schema_path), generated when `format_schema_source` is set to `query`.
Note: If no target is specified, both caches are cleared.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

## SYSTEM FLUSH LOGS {#flush-logs}

Flushes buffered log messages to system tables, e.g. system.query_log. Mainly useful for debugging since most system tables have a default flush interval of 7.5 seconds.
This will also create system tables even if message queue is empty.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

If you don't want to flush everything, you can flush one or more individual logs by passing either their name or their target table:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

## SYSTEM RELOAD CONFIG {#reload-config}

Reloads ClickHouse configuration. Used when configuration is stored in ZooKeeper. Note that `SYSTEM RELOAD CONFIG` does not reload `USER` configuration stored in ZooKeeper, it only reloads `USER` configuration that is stored in `users.xml`.  To reload all `USER` config use `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

## SYSTEM RELOAD USERS {#reload-users}

Reloads all access storages, including: users.xml, local disk access storage, replicated (in ZooKeeper) access storage.

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

## SYSTEM SHUTDOWN {#shutdown}

<CloudNotSupportedBadge/>

Normally shuts down ClickHouse (like `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## SYSTEM KILL {#kill}

Aborts ClickHouse process (like `kill -9 {$ pid_clickhouse-server}`)

## SYSTEM INSTRUMENT {#instrument}

Manages instrumentation points using LLVM's XRay feature which is available when ClickHouse is built using `ENABLE_XRAY=1`.
This enables to debug and profile in production without modifying the source code and with minimal overhead.
When no instrumentation point is added, the performance penalty is negligible because it only adds an extra jump to a nearby
address at the prolog and epilog of those functions that are longer than 200 instructions.

### SYSTEM INSTRUMENT ADD {#instrument-add}

Adds a new instrumentation point. Functions instrumented can be inspected in the [`system.instrumentation`](/reference/system-tables/instrumentation) system table. More than one handler can be added for the same function, and they will be executed in the same order the instrumentation is added.
The functions to be instrumented can be collected from [`system.symbols`](/reference/system-tables/symbols) system table.

There are three different kind of handlers to add to functions:

**Syntax**
```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

where `FUNCTION` is any function or substring of a function such as `QueryMetricLog::startQuery`, and the handler one of the following

#### LOG {#instrument-add-log}

Prints the text provided as an argument and the stack trace either on `ENTRY` or `EXIT` of the function.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

#### SLEEP {#instrument-add-sleep}

Sleeps for a number of fix amount of seconds either on `ENTRY` or `EXIT`:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

or for a uniformly distributed random amount of seconds providing min and max separated by a whitespace:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

#### PROFILE {#instrument-add-profile}

Measures the time spent between `ENTRY` and `EXIT` of a function.
The result of the profiling is stored in [`system.trace_log`](/reference/system-tables/trace_log) and can be converted
to [Chrome Event Trace Format](/reference/system-tables/trace_log#chrome-event-trace-format).

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

### SYSTEM INSTRUMENT REMOVE {#instrument-remove}

Removes either a single instrumentation point with:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

all of them using the `ALL` keyword:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

a set of IDs from a subquery:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

or all instrumentation points that match a given function_name:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

The instrumentation point information can be collected from [`system.instrumentation`](/reference/system-tables/instrumentation) system table.

## Managing Distributed Tables {#managing-distributed-tables}

ClickHouse can manage [distributed](/reference/engines/table-engines/special/distributed) tables. When a user inserts data into these tables, ClickHouse first creates a queue of the data that should be sent to cluster nodes, then asynchronously sends it. You can manage queue processing with the [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed), and [`START DISTRIBUTED SENDS`](#start-distributed-sends) queries. You can also synchronously insert distributed data with the [`distributed_foreground_insert`](/reference/settings/session-settings/distributed#distributed_foreground_insert) setting.

### SYSTEM STOP DISTRIBUTED SENDS {#stop-distributed-sends}

Disables background data distribution when inserting data into distributed tables.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<Note>
In case of [`prefer_localhost_replica`](/reference/settings/session-settings/prefer#prefer_localhost_replica) is enabled (the default), the data to local shard will be inserted anyway.
</Note>

### SYSTEM FLUSH DISTRIBUTED {#flush-distributed}

Forces ClickHouse to send data to cluster nodes synchronously. If any nodes are unavailable, ClickHouse throws an exception and stops query execution. You can retry the query until it succeeds, which will happen when all nodes are back online.

You can also override some settings via `SETTINGS` clause, this can be useful to avoid some temporary limitations, like `max_concurrent_queries_for_all_users` or `max_memory_usage`.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

<Note>
Each pending block is stored in disk with settings from the initial INSERT query, so that is why sometimes you may want to override settings.
</Note>

### SYSTEM START DISTRIBUTED SENDS {#start-distributed-sends}

Enables background data distribution when inserting data into distributed tables.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

### SYSTEM STOP LISTEN {#stop-listen}

Closes the socket and gracefully terminates the existing connections to the server on the specified port with the specified protocol.

However, if the corresponding protocol settings were not specified in the clickhouse-server configuration, this command will have no effect.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

- If `CUSTOM 'protocol'` modifier is specified, the custom protocol with the specified name defined in the protocols section of the server configuration will be stopped.
- If `QUERIES ALL [EXCEPT .. [,..]]` modifier is specified, all protocols are stopped, unless specified with `EXCEPT` clause.
- If `QUERIES DEFAULT [EXCEPT .. [,..]]` modifier is specified, all default protocols are stopped, unless specified with `EXCEPT` clause.
- If `QUERIES CUSTOM [EXCEPT .. [,..]]` modifier is specified, all custom protocols are stopped, unless specified with `EXCEPT` clause.

### SYSTEM START LISTEN {#start-listen}

Allows new connections to be established on the specified protocols.

However, if the server on the specified port and protocol was not stopped using the SYSTEM STOP LISTEN command, this command will have no effect.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

## Managing MergeTree Tables {#managing-mergetree-tables}

ClickHouse can manage background processes in [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) tables.

### SYSTEM STOP MERGES {#stop-merges}

<CloudNotSupportedBadge/>

Provides possibility to stop background merges for tables in the MergeTree family:

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<Note>
`DETACH / ATTACH` table will start background merges for the table even in case when merges have been stopped for all MergeTree tables before.
</Note>

### SYSTEM START MERGES {#start-merges}

<CloudNotSupportedBadge/>

Provides possibility to start background merges for tables in the MergeTree family:

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

### SYSTEM STOP TTL MERGES {#stop-ttl-merges}

<CloudNotSupportedBadge/>

Provides possibility to stop background delete old data according to [TTL expression](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist or table has not MergeTree engine. Returns error when database does not exist:

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM START TTL MERGES {#start-ttl-merges}

<CloudNotSupportedBadge/>

Provides possibility to start background delete old data according to [TTL expression](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM STOP MOVES {#stop-moves}

Provides possibility to stop background move data according to [TTL table expression with TO VOLUME or TO DISK clause](/reference/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM START MOVES {#start-moves}

Provides possibility to start background move data according to [TTL table expression with TO VOLUME and TO DISK clause](/reference/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl) for tables in the MergeTree family:
Returns `Ok.` even if table does not exist. Returns error when database does not exist:

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

### SYSTEM UNFREEZE {#query_language-system-unfreeze}

Clears a frozen backup with the specified name from all the disks. See more about unfreezing separate parts in [ALTER TABLE table_name UNFREEZE WITH NAME ](/reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

### SYSTEM WAIT LOADING PARTS {#wait-loading-parts}

Wait until all asynchronously loading data parts of a table (outdated data parts) will became loaded.

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

## Managing ReplicatedMergeTree Tables {#managing-replicatedmergetree-tables}

ClickHouse can manage background replication related processes in [ReplicatedMergeTree](/reference/engines/table-engines/mergetree-family/replication) tables.

### SYSTEM STOP FETCHES {#stop-fetches}

<CloudNotSupportedBadge/>

Provides possibility to stop background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even if table or database does not exist.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START FETCHES {#start-fetches}

<CloudNotSupportedBadge/>

Provides possibility to start background fetches for inserted parts for tables in the `ReplicatedMergeTree` family:
Always returns `Ok.` regardless of the table engine and even if table or database does not exist.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM STOP REPLICATED SENDS {#stop-replicated-sends}

Provides possibility to stop background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START REPLICATED SENDS {#start-replicated-sends}

Provides possibility to start background sends to other replicas in cluster for new inserted parts for tables in the `ReplicatedMergeTree` family:

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM STOP REPLICATION QUEUES {#stop-replication-queues}

Provides possibility to stop background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START REPLICATION QUEUES {#start-replication-queues}

Provides possibility to start background fetch tasks from replication queues which stored in Zookeeper for tables in the `ReplicatedMergeTree` family. Possible background tasks types - merges, fetches, mutation, DDL statements with ON CLUSTER clause:

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM STOP PULLING REPLICATION LOG {#stop-pulling-replication-log}

Stops loading new entries from replication log to replication queue in a `ReplicatedMergeTree` table.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM START PULLING REPLICATION LOG {#start-pulling-replication-log}

Cancels `SYSTEM STOP PULLING REPLICATION LOG`.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

### SYSTEM SYNC REPLICA {#sync-replica}

Wait until a `ReplicatedMergeTree` table will be synced with other replicas in a cluster, but no more than `receive_timeout` seconds.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

After running this statement the `[db.]replicated_merge_tree_family_table_name` fetches commands from the common replicated log into its own replication queue, and then the query waits till the replica processes all of the fetched commands. The following modifiers are supported:

- With `IF EXISTS` (available since 25.6) the query won't throw an error if the table does not exists. This is useful when adding a new replica to a cluster, when it's already part of the cluster configuration but it is still in the process of creating and synchronizing the table.
- If a `STRICT` modifier was specified then the query waits for the replication queue to become empty. The `STRICT` version may never succeed if new entries constantly appear in the replication queue.
- If a `LIGHTWEIGHT` modifier was specified then the query waits only for `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE` and `DROP_PART` entries to be processed.
  Additionally, the LIGHTWEIGHT modifier supports an optional FROM 'srcReplicas' clause, where 'srcReplicas' is a comma-separated list of source replica names. This extension allows for more targeted synchronization by focusing only on replication tasks originating from the specified source replicas.
- If a `PULL` modifier was specified then the query pulls new replication queue entries from ZooKeeper, but does not wait for anything to be processed.

### SYNC DATABASE REPLICA {#sync-database-replica}

Waits until the specified [replicated database](/reference/engines/database-engines/replicated) applies all schema changes from the DDL queue of that database.

**Syntax**
```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

### SYSTEM RESTART REPLICA {#restart-replica}

Provides possibility to reinitialize Zookeeper session's state for `ReplicatedMergeTree` table, will compare current state with Zookeeper as source of truth and add tasks to Zookeeper queue if needed.
Initialization of replication queue based on ZooKeeper data happens in the same way as for `ATTACH TABLE` statement. For a short time, the table will be unavailable for any operations.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

### SYSTEM RESTORE REPLICA {#restore-replica}

Restores a replica if data is [possibly] present but Zookeeper metadata is lost.

Works only on readonly `ReplicatedMergeTree` tables.

One may execute query after:

- ZooKeeper root `/` loss.
- Replicas path `/replicas` loss.
- Individual replica path `/replicas/replica_name/` loss.

Replica attaches locally found parts and sends info about them to Zookeeper.
Parts present on a replica before metadata loss are not re-fetched from other ones if not being outdated (so replica restoration does not mean re-downloading all data over the network).

<Note>
Parts in all states are moved to `detached/` folder. Parts active before data loss (committed) are attached.
</Note>

### SYSTEM RESTORE DATABASE REPLICA {#restore-database-replica}

Restores a replica if data is [possibly] present but Zookeeper metadata is lost.

**Syntax**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**Example**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**Syntax**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

Alternative syntax:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**Example**

Creating a table on multiple servers. After the replica's metadata in ZooKeeper is lost, the table will attach as read-only as metadata is missing. The last query needs to execute on every replica.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

Another way:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

### SYSTEM RESTART REPLICAS {#restart-replicas}

Provides possibility to reinitialize Zookeeper sessions state for all `ReplicatedMergeTree` tables, will compare current state with Zookeeper as source of true and add tasks to Zookeeper queue if needed

### SYSTEM CLEAR|DROP FILESYSTEM CACHE {#drop-filesystem-cache}

Allows to drop filesystem cache.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

### SYSTEM SYNC FILE CACHE {#sync-file-cache}

<Note>
It's too heavy and has potential for misuse.
</Note>

Will do sync syscall.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

### SYSTEM LOAD PRIMARY KEY {#load-primary-key}

Load the primary keys for the given table or for all tables.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

### SYSTEM UNLOAD PRIMARY KEY {#unload-primary-key}

Unload the primary keys for the given table or for all tables.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

## Managing Refreshable Materialized Views {#managing-refreshable-materialized-views}

Commands to control background tasks performed by [Refreshable Materialized Views](/reference/statements/create/view#refreshable-materialized-view)

Keep an eye on [`system.view_refreshes`](/reference/system-tables/view_refreshes) while using them.

### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS {#stop-view-stop-views}

Disable periodic refreshing of the given view or all refreshable views. If a refresh is in progress, cancel it too.

If the view is in a Replicated or Shared database, `STOP VIEW` only affects the current replica, while `STOP REPLICATED VIEW` affects all replicas.

<Note>
The stopped state does not persist across server restarts. After a restart, views will resume their configured refresh schedules.
In Replicated or Shared databases, `SYSTEM STOP VIEW` only affects the current replica. Use `SYSTEM STOP REPLICATED VIEW` to stop refreshes on all replicas.
</Note>

```sql
SYSTEM STOP VIEW [db.]name
```
```sql
SYSTEM STOP VIEWS
```

### SYSTEM START [REPLICATED] VIEW, START VIEWS {#start-view-start-views}

Enable periodic refreshing for the given view or all refreshable views. No immediate refresh is triggered.

If the view is in a Replicated or Shared database, `START VIEW` undoes the effect of `STOP VIEW`, and `START REPLICATED VIEW` undoes the effect of `STOP REPLICATED VIEW`. `START VIEW` also undoes the effect of `PAUSE VIEW`.

```sql
SYSTEM START VIEW [db.]name
```
```sql
SYSTEM START VIEWS
```

### SYSTEM PAUSE VIEW, PAUSE VIEWS {#pause-view-pause-views}

Disable periodic refreshing of the given view or all refreshable views.
Unlike `SYSTEM STOP VIEW`, `SYSTEM PAUSE VIEW` does not interrupt a refresh that is already in progress: the running refresh is allowed to finish, and only subsequent refreshes are prevented.

Undo with `SYSTEM START VIEW` or `SYSTEM START VIEWS`.

<Note>
The paused state does not persist across server restarts. After a restart, views will resume their configured refresh schedules.
In Replicated or Shared databases, `SYSTEM PAUSE VIEW` only affects the current replica.
</Note>

```sql
SYSTEM PAUSE VIEW [db.]name
```
```sql
SYSTEM PAUSE VIEWS
```

### SYSTEM REFRESH VIEW {#refresh-view}

Trigger an immediate out-of-schedule refresh of a given view.

```sql
SYSTEM REFRESH VIEW [db.]name
```

### SYSTEM WAIT VIEW {#wait-view}

Waits for the running refresh to complete. If no refresh is running, returns immediately. If the latest refresh attempt failed, reports an error.

Can be used right after creating a new refreshable materialized view (without EMPTY keyword) to wait for the initial refresh to complete.

If the view is in a Replicated or Shared database, and refresh is running on another replica, waits for that refresh to complete.

```sql
SYSTEM WAIT VIEW [db.]name
```

### SYSTEM CANCEL VIEW {#cancel-view}

If there's a refresh in progress for the given view on the current replica, interrupt and cancel it. Otherwise do nothing.

```sql
SYSTEM CANCEL VIEW [db.]name
```

## Managing Background Activity {#managing-background-activity}

Engine-agnostic commands to control the background activity of a single table, or of every such table on the server at once. They cover:

- [Refreshable materialized views](/reference/statements/create/view#refreshable-materialized-view) (the periodic refresh), and
- the streaming table engines that continuously consume from an external source: [Kafka](/reference/engines/table-engines/integrations/kafka), [RabbitMQ](/reference/engines/table-engines/integrations/rabbitmq), [NATS](/reference/engines/table-engines/integrations/nats), [S3Queue](/reference/engines/table-engines/integrations/s3queue) and [AzureQueue](/reference/engines/table-engines/integrations/azure-queue).

For a refreshable materialized view each verb is an alias of the corresponding `SYSTEM ... VIEW` command from [Managing Refreshable Materialized Views](#managing-refreshable-materialized-views), so `SYSTEM STOP [db.]name` behaves exactly like `SYSTEM STOP VIEW [db.]name`, and so on.

The per-table and wildcard forms differ in how they treat tables without a background activity. The per-table form (`SYSTEM STOP [db.]table`) throws an error if the named table is neither a streaming engine nor a refreshable materialized view. The wildcard form silently skips such tables, so it is always safe to run.

`STOP` and `CANCEL` interrupt consumption as soon as possible. For [Kafka](/reference/engines/table-engines/integrations/kafka), [RabbitMQ](/reference/engines/table-engines/integrations/rabbitmq) and [NATS](/reference/engines/table-engines/integrations/nats) they stop reading from the source but do not interrupt an insert that has already started: a block already being written into the materialized views still finishes and commits. [S3Queue](/reference/engines/table-engines/integrations/s3queue) and [AzureQueue](/reference/engines/table-engines/integrations/azure-queue) read and insert in a single pipeline. With deduplication enabled (default) the insert is cancelled too and the files are reprocessed later. With deduplication disabled the in-flight batch finishes and commits instead (like the engines above) to avoid duplicating rows. Data that was read but not yet committed is consumed again later, so nothing is lost, except for core NATS (without JetStream), which cannot redeliver and drops it.

`PAUSE` does not interrupt a running insert, so it normally does not lose anything. Core NATS is the exception: pausing stops consuming and drops the messages it had already received but not yet inserted, and core NATS cannot redeliver them.

<Note>
None of these states persist across a server restart. After a restart, refreshable views resume their configured schedules and streaming engines resume consuming.
</Note>

### SYSTEM STOP {#stop-background}

Stop the background activity and keep it stopped: interrupt what is running now, and run nothing further until `SYSTEM START`. Equivalent to `PAUSE` + `CANCEL`.

```sql
SYSTEM STOP [db.]table
SYSTEM STOP ALL BACKGROUND
```

### SYSTEM START {#start-background}

Resume activity, undoing a previous `SYSTEM STOP` or `SYSTEM PAUSE`. No activity is interrupted.

```sql
SYSTEM START [db.]table
SYSTEM START ALL BACKGROUND
```

### SYSTEM PAUSE {#pause-background}

Prevent further background activity, but let whatever is running right now finish first.

```sql
SYSTEM PAUSE [db.]table
SYSTEM PAUSE ALL BACKGROUND
```

### SYSTEM CANCEL {#cancel-background}

Interrupt the activity running right now only, without blocking future activity — the table keeps refreshing or consuming on its schedule. Does nothing if no activity is in progress.

```sql
SYSTEM CANCEL [db.]table
SYSTEM CANCEL ALL BACKGROUND
```

### SYSTEM REFRESH {#refresh-background}

Run one extra cycle out of schedule. On a streaming table it runs immediately and once, even while the table is stopped or paused. On a refreshable materialized view it behaves like `SYSTEM REFRESH VIEW`: if the view is stopped, the refresh is remembered and runs once `SYSTEM START` releases it.

```sql
SYSTEM REFRESH [db.]table
SYSTEM REFRESH ALL BACKGROUND
```

### Privileges {#background-privileges}

Each command requires the privilege of the targeted engine: `SYSTEM VIEWS` for a refreshable materialized view and `SYSTEM STREAMING ENGINES` for a streaming table. Both are children of `SYSTEM BACKGROUND`, so granting `SYSTEM BACKGROUND` allows controlling the background activity of every such table. The `ALL BACKGROUND` forms apply only to the tables the user is allowed to control and silently skip the rest.

## SYSTEM FLUSH OBJECT STORAGE QUEUE {#flush-object-storage-queue}

Blocks until the given file has been processed or permanently failed by the given [S3Queue](/reference/engines/table-engines/integrations/s3queue) or [AzureQueue](/reference/engines/table-engines/integrations/azure-queue) table. Returns immediately if the file was already processed. Raises an error if the file has permanently failed (all retries exhausted).

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```
)DOCS_MD",
        .syntax = R"(
SYSTEM RELOAD CONFIG | USERS | FUNCTIONS | ASYNCHRONOUS METRICS
SYSTEM RELOAD [EMBEDDED] DICTIONARIES | DICTIONARY [db.]name
SYSTEM UNLOAD DICTIONARIES | DICTIONARY [db.]name
SYSTEM [CLEAR|DROP] ... CACHE [ON CLUSTER cluster_name]
SYSTEM PREWARM MARK CACHE | PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
SYSTEM DROP REPLICA 'replica_name' [FROM TABLE|DATABASE|ZKPATH ...]
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] [FROM DATABASE|ZKPATH ...]
SYSTEM FLUSH LOGS [table [, ...]]
SYSTEM FLUSH DISTRIBUTED [db.]name
SYSTEM SHUTDOWN | KILL
SYSTEM INSTRUMENT ...
SYSTEM START|STOP DISTRIBUTED SENDS | MERGES | TTL MERGES | MOVES | FETCHES | REPLICATED SENDS | REPLICATION QUEUES | PULLING REPLICATION LOG | LISTEN | VIEW | VIEWS
SYSTEM SYNC REPLICA | DATABASE REPLICA | TRANSACTION LOG | FILE CACHE | FILESYSTEM CACHE
SYSTEM RESTART REPLICA | RESTORE REPLICA [db.]name
SYSTEM REFRESH VIEW | WAIT VIEW | CANCEL VIEW [db.]name
SYSTEM UNFREEZE WITH NAME 'backup_name'
SYSTEM FLUSH OBJECT STORAGE QUEUE
)",
        .related = {"KILL", "OPTIMIZE", "ALTER", "SHOW", "ON CLUSTER"},
    });
}

}
