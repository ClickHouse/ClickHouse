#include <Interpreters/DatabaseReplicator.h>
#include <Interpreters/DDLReplicateWorker.h>
#include <Interpreters/DatabaseReplicatorSettings.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/DDLTask.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Databases/IDatabase.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <base/scope_guard.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_ZOOKEEPER;
    extern const int INCORRECT_DATA;
    extern const int QUERY_IS_PROHIBITED;
    extern const int NOT_IMPLEMENTED;
    extern const int DATABASE_REPLICATION_FAILED;
    extern const int INCORRECT_QUERY;
}

namespace Setting
{
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ServerSetting
{
    extern const ServerSettingsBool allow_experimental_database_replicator;
    extern const ServerSettingsString default_database;
}

namespace DatabaseReplicatorSetting
{
    extern const DatabaseReplicatorSettingsString zookeeper_path;
    extern const DatabaseReplicatorSettingsString shard_name;
    extern const DatabaseReplicatorSettingsString replica_name;
}

static constexpr const char * DATABASE_REPLICATOR_MARK = "DatabaseReplicator";

static inline UInt64 getMetadataHash(const String & database_name, const String & metadata)
{
    SipHash hash;
    hash.update(database_name);
    hash.update(metadata);
    return hash.get64();
}

DatabaseReplicator::DatabaseReplicator(
    ContextMutablePtr context_,
    const String & zookeeper_name_,
    const String & zookeeper_path_,
    const String & shard_name_,
    const String & replica_name_,
    DatabaseReplicatorSettings settings_)
    : WithMutableContext(context_)
    , DDLReplicator(zookeeper_name_, zookeeper_path_, shard_name_, replica_name_)
    , settings(std::move(settings_))
    , log(::getLogger("DatabaseReplicator"))
{
}

std::unique_ptr<DatabaseReplicator> DatabaseReplicator::database_replicator;

void DatabaseReplicator::init(ContextMutablePtr global_context_, const Poco::Util::AbstractConfiguration & config)
{
    if (!global_context_->getServerSettings()[ServerSetting::allow_experimental_database_replicator])
        return;

    if (database_replicator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database catalog is initialized twice. This is a bug.");

    DatabaseReplicatorSettings settings;
    settings.loadFromConfig("database_replicator", config);

    String zookeeper_path = settings[DatabaseReplicatorSetting::zookeeper_path];
    auto zookeeper_name = zkutil::extractZooKeeperName(zookeeper_path);
    zookeeper_path = zkutil::extractZooKeeperPath(zookeeper_path, /*check_starts_with_slash*/false);
    String shard_name = settings[DatabaseReplicatorSetting::shard_name];
    String replica_name = settings[DatabaseReplicatorSetting::replica_name];

    Macros::MacroExpansionInfo info;
    info.level = 0;
    shard_name = global_context_->getMacros()->expand(shard_name, info);
    info.level = 0;
    replica_name = global_context_->getMacros()->expand(replica_name, info);

    database_replicator = std::make_unique<DatabaseReplicator>(
        global_context_,
        zookeeper_name,
        zookeeper_path,
        shard_name,
        replica_name,
        std::move(settings));
    database_replicator->tryConnectToZooKeeperAndInit();
}

DatabaseReplicator & DatabaseReplicator::instance()
{
    if (!database_replicator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database replicator is not initialized. This is a bug.");
    return *database_replicator;
}

bool DatabaseReplicator::isEnabled()
{
    return database_replicator != nullptr;
}

void DatabaseReplicator::shutdown()
{
    database_replicator.reset();
}

String DatabaseReplicator::getName() const
{
    return DATABASE_REPLICATOR_MARK;
}

const DDLReplicatorSettings & DatabaseReplicator::getSettings() const
{
    return settings.getDDLReplicatorSettings();
}

void DatabaseReplicator::startup()
{
    std::lock_guard lock{ddl_worker_mutex};
    if (ddl_worker || ddl_worker_initialized)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DDL worker of database replicator is initialized twice. This is a bug.");
    }
    ddl_worker = std::make_unique<DDLReplicateWorker>(this, getContext(), "DDLWorker(DatabaseReplicator)");
    ddl_worker->startup();
    ddl_worker_initialized = true;
}

UInt64 DatabaseReplicator::getLocalDigest() const
{
    UInt64 local_digest = 0;
    {
        std::lock_guard lock{metadata_mutex};
        for (const auto & [_, digest] : local_digests)
            local_digest += digest;
    }
    return local_digest;
}

void DatabaseReplicator::tryConnectToZooKeeperAndInit()
{
    if (!getContext()->hasZooKeeper())
    {
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "Can't create replicated database without ZooKeeper");
    }

    auto current_zookeeper = getZooKeeper(getContext());
    if (!current_zookeeper->exists(zookeeper_path))
        createDDLReplicatorNodesInZooKeeper(current_zookeeper);

    String replica_host_id;
    bool replica_exists_in_zk = current_zookeeper->tryGet(replica_path, replica_host_id);

    LOG_TEST(log, "Replica {} exists in Keeper {}", replica_path, replica_exists_in_zk);

    if (replica_exists_in_zk)
    {
        String host_id = getHostID(getContext(), false);

        if (replica_host_id != host_id)
        {
            // After restarting, InterserverIOAddress might change (e.g: config updated, `getFQDNOrHostName` returns a different one)
            // If the UUID in the keeper is the same as the current server UUID, we will update the host_id in keeper
            LOG_INFO(
                log,
                "Database replicator replica: {}, shard {}, zk_path: {} already exists with the same UUID, replica host ID: '{}', "
                "current host ID: '{}', will set the host_id to the current host ID",
                replica_name,
                shard_name,
                zookeeper_path,
                replica_host_id,
                host_id);
            current_zookeeper->set(replica_path, host_id, -1);
            createEmptyLogEntry(current_zookeeper);
        }

        /// Check that replica_group_name in ZooKeeper matches the local one and change it if necessary.
        String zk_replica_group_name;
        if (!current_zookeeper->tryGet(replica_path + "/replica_group", zk_replica_group_name))
        {
            current_zookeeper->create(replica_path + "/replica_group", replica_group_name, zkutil::CreateMode::Persistent);
            if (!replica_group_name.empty())
                createEmptyLogEntry(current_zookeeper);
        }
        else if (zk_replica_group_name != replica_group_name)
        {
            current_zookeeper->set(replica_path + "/replica_group", replica_group_name, -1);
            createEmptyLogEntry(current_zookeeper);
        }

        /// Needed to mark all the queries
        /// in the range (max log ptr at replica ZooKeeper nodes creation, max log ptr after replica recovery] as successful.
        String max_log_ptr_at_creation_str;
        if (current_zookeeper->tryGet(replica_path + "/max_log_ptr_at_creation", max_log_ptr_at_creation_str))
            max_log_ptr_at_creation = parse<UInt32>(max_log_ptr_at_creation_str);
    }
    else
    {
        /// Create replica nodes in ZooKeeper. If newly initialized nodes already exist, reuse them.
        createReplicaNodesInZooKeeper(getContext(), current_zookeeper);
    }
    is_readonly = false;
}

void DatabaseReplicator::initializeLocalDigest()
{
    std::lock_guard lock{metadata_mutex};
    metadata_digest = 0;
    local_digests.clear();

    auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});

    for (const auto & [database_name, database] : databases)
    {
        if (!canReplicateDatabase(database_name))
            continue;
        auto statement = getCreateDatabaseStatement(database);
        UInt64 digest = getMetadataHash(database_name, statement);
        local_digests[database_name] = digest;
        metadata_digest += digest;
    }

    LOG_INFO(log, "Initialized local digest: {} databases, digest={}", local_digests.size(), metadata_digest);
}

bool DatabaseReplicator::canReplicateDatabase(const String & database_name) const
{
    return !DatabaseCatalog::isPredefinedDatabase(database_name)
        && database_name != getContext()->getServerSettings()[ServerSetting::default_database].value;
}

bool DatabaseReplicator::shouldReplicateQuery(const ContextPtr & query_context, const ASTPtr & query_ptr) const
{
    /// Queries that are already internal (replayed from the DDL log) must not be forwarded again.
    if (query_context->getClientInfo().is_replicated_database_internal)
        return false;

    /// CREATE DATABASE — replicate if the target database is eligible.
    if (const auto * create = query_ptr->as<const ASTCreateQuery>())
    {
        /// Only CREATE DATABASE (has `database`, no `table`).
        if (create->database && !create->table)
            return canReplicateDatabase(create->getDatabase());
        return false;
    }

    /// DROP / DETACH DATABASE — replicate DROP only; DETACH is prohibited for replicable databases.
    if (const auto * drop = query_ptr->as<const ASTDropQuery>())
    {
        if (drop->database && !drop->table)
        {
            if (drop->kind == ASTDropQuery::Kind::Detach && canReplicateDatabase(drop->getDatabase()))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "DETACH DATABASE is not allowed when DatabaseReplicator is enabled");
            if (drop->kind == ASTDropQuery::Kind::Drop)
                return canReplicateDatabase(drop->getDatabase());
        }
        return false;
    }

    /// ALTER DATABASE — replicate if the target database is eligible.
    if (const auto * alter = query_ptr->as<const ASTAlterQuery>())
    {
        if (alter->alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
            return canReplicateDatabase(alter->getDatabase());
        return false;
    }

    /// RENAME DATABASE — replicate if both source and target databases are eligible.
    /// If one is replicable but the other is not, it is an error.
    if (const auto * rename = query_ptr->as<const ASTRenameQuery>())
    {
        if (rename->database && !rename->getElements().empty())
        {
            const auto & elem = rename->getElements().front();
            bool from_replicable = canReplicateDatabase(elem.from.getDatabase());
            bool to_replicable = canReplicateDatabase(elem.to.getDatabase());
            if (from_replicable != to_replicable)
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "Cannot rename database '{}' to '{}': both databases must be replicable or both non-replicable "
                    "when DatabaseReplicator is enabled",
                    elem.from.getDatabase(),
                    elem.to.getDatabase());
            return from_replicable && to_replicable;
        }
        return false;
    }

    /// All other query types are not handled by DatabaseReplicator.
    return false;
}

BlockIO DatabaseReplicator::tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context, QueryFlags flags)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseReplicator::tryEnqueueReplicatedDDL");

    if (!DatabaseCatalog::instance().canPerformReplicatedDDLQueries())
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Replicated DDL queries are disabled");

    if (query_context->getCurrentTransaction() && query_context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Distributed DDL queries inside transactions are not supported");

    if (is_readonly)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "DatabaseReplicator is in readonly mode, because it cannot connect to ZooKeeper");

    String host_fqdn_id;
    {
        std::lock_guard lock{ddl_worker_mutex};
        if (!ddl_worker)
            throw Exception(ErrorCodes::DATABASE_REPLICATION_FAILED, "DatabaseReplicator DDL worker is not initialized");
        host_fqdn_id = ddl_worker->getCommonHostID();
    }

    LOG_DEBUG(log, "Proposing query: {}", query->formatForLogging());

    DDLLogEntry entry;
    entry.query = query->formatWithSecretsOneLine();
    entry.initiator = host_fqdn_id;
    entry.setSettingsIfRequired(query_context);
    entry.tracing_context = OpenTelemetry::CurrentContext();
    entry.initial_query_id = query_context->getClientInfo().initial_query_id;
    entry.is_backup_restore = flags.distributed_backup_restore;
    String node_path = ddl_worker->tryEnqueueAndExecuteEntry(entry, query_context, flags.internal);

    Strings hosts_to_wait;
    Strings unfiltered_hosts = getZooKeeper(getContext())->getChildren(zookeeper_path + "/replicas");

    std::vector<String> paths;
    for (const auto & host : unfiltered_hosts)
        paths.push_back(zookeeper_path + "/replicas/" + host + "/replica_group");

    auto replica_groups = getZooKeeper(getContext())->tryGet(paths);

    for (size_t i = 0; i < paths.size(); ++i)
    {
        if (replica_groups[i].data == replica_group_name)
            hosts_to_wait.push_back(unfiltered_hosts[i]);
    }

    return getQueryStatus(
        zookeeper_name,
        node_path,
        fs::path(zookeeper_path) / "replicas",
        query_context,
        hosts_to_wait,
        /*database_guard=*/nullptr);
}

void DatabaseReplicator::commitCreateDatabase(const String & database_name, ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();

    auto statement = getCreateDatabaseStatement(database_name);
    UInt64 new_database_digest = getMetadataHash(database_name, statement);

    if (txn && txn->isInitialQuery() && !txn->isCreateOrReplaceQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(database_name);
        txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, statement, zkutil::CreateMode::Persistent));
    }

    std::lock_guard lock{metadata_mutex};
    if (local_digests.contains(database_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "commitCreateDatabase: database {} already exists", database_name);
    UInt64 new_digest = metadata_digest + new_database_digest;

    if (txn && !txn->isCreateOrReplaceQuery() && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));
    if (txn && !query_context->isInternalSubquery())
        txn->commit();

    local_digests[database_name] = new_database_digest;
    metadata_digest = new_digest;
    assertDigest(query_context);
}

void DatabaseReplicator::commitDropDatabase(const String & database_name, ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();

    if (txn && txn->isInitialQuery() && !txn->isCreateOrReplaceQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(database_name);
        txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, -1));
    }

    std::lock_guard lock{metadata_mutex};
    auto it = local_digests.find(database_name);
    if (it == local_digests.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "commitDropDatabase: database {} does not exist", database_name);
    auto old_database_digest = it->second;
    UInt64 new_digest = metadata_digest - old_database_digest;

    if (txn && !txn->isCreateOrReplaceQuery() && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));
    if (txn && !query_context->isInternalSubquery())
        txn->commit();

    metadata_digest = new_digest;
    local_digests.erase(it);
    assertDigest(query_context);
}

void DatabaseReplicator::commitAlterDatabase(const String & database_name, ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();

    auto statement = getCreateDatabaseStatement(database_name);
    UInt64 new_database_digest = getMetadataHash(database_name, statement);

    if (txn && txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(database_name);
        txn->addOp(zkutil::makeSetRequest(metadata_zk_path, statement, -1));
    }

    std::lock_guard lock{metadata_mutex};
    UInt64 old_database_digest = 0;
    auto it = local_digests.find(database_name);
    if (it == local_digests.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "commitAlterDatabase: database {} does not exist", database_name);
    old_database_digest = it->second;
    UInt64 new_digest = metadata_digest - old_database_digest + new_database_digest;

    if (txn && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));
    if (txn && !query_context->isInternalSubquery())
        txn->commit();

    local_digests[database_name] = new_database_digest;
    metadata_digest = new_digest;
    assertDigest(query_context);
}

void DatabaseReplicator::commitRenameDatabase(
    const String & database_name,
    const String & to_database_name,
    bool exchange,
    ContextPtr query_context)
{
    auto txn = query_context->getZooKeeperMetadataTransaction();
    assert(txn);

    if (database_name == to_database_name)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot rename database to itself");

    /// The database is already renamed locally.
    /// So we should get the new statement by to_database_name
    String statement_renamed = getCreateDatabaseStatement(to_database_name);
    String statement_exchanged;
    if (exchange)
        statement_exchanged = getCreateDatabaseStatement(database_name);

    if (txn->isInitialQuery())
    {
        String metadata_zk_path = zookeeper_path + "/metadata/" + escapeForFileName(database_name);
        String metadata_zk_path_to = zookeeper_path + "/metadata/" + escapeForFileName(to_database_name);

        String zk_statement;
        Coordination::Stat stat;
        String zk_statement_to;
        Coordination::Stat stat_to;

        auto zookeeper = txn->getZooKeeper();
        zookeeper->tryGet(metadata_zk_path, zk_statement, &stat);
        zookeeper->tryGet(metadata_zk_path_to, zk_statement_to, &stat_to);

        if (!txn->isCreateOrReplaceQuery())
            txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path, stat.version));

        if (exchange)
        {
            txn->addOp(zkutil::makeRemoveRequest(metadata_zk_path_to, stat_to.version));
            if (!txn->isCreateOrReplaceQuery())
                txn->addOp(zkutil::makeCreateRequest(metadata_zk_path, zk_statement_to, zkutil::CreateMode::Persistent));
        }

        /// In case of CREATE OR REPLACE there is no statement for the temporary table in ZK, so we use the local definition
        if (txn->isCreateOrReplaceQuery())
            txn->addOp(zkutil::makeCreateRequest(metadata_zk_path_to, statement_renamed, zkutil::CreateMode::Persistent));
        else
            txn->addOp(zkutil::makeCreateRequest(metadata_zk_path_to, zk_statement, zkutil::CreateMode::Persistent));
    }

    auto digest_renamed = DB::getMetadataHash(to_database_name, statement_renamed);
    auto digest_exchanged = DB::getMetadataHash(database_name, statement_exchanged);

    std::lock_guard lock{metadata_mutex};
    auto from_it = local_digests.find(database_name);
    if (from_it == local_digests.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "commitRenameDatabase: database {} does not exist", database_name);
    if (!exchange && local_digests.contains(to_database_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "commitRenameDatabase: database {} already exists", to_database_name);
    UInt64 new_digest = metadata_digest;
    new_digest -= from_it->second;
    new_digest += digest_renamed;
    if (exchange)
    {
        new_digest -= DB::getMetadataHash(to_database_name, statement_exchanged);
        new_digest += digest_exchanged;
    }
    if (txn && !is_recovering)
        txn->addOp(zkutil::makeSetRequest(replica_path + "/digest", toString(new_digest), -1));

    if (txn && !query_context->isInternalSubquery())
        txn->commit();

    metadata_digest = new_digest;
    local_digests[to_database_name] = digest_renamed;
    if (exchange)
        from_it->second = digest_exchanged;
    else
        local_digests.erase(from_it);
    assertDigest(query_context);
}

String DatabaseReplicator::getCreateDatabaseStatement(const DatabasePtr & database)
{
    ASTPtr create_query;
    try
    {
        create_query = database->getCreateDatabaseQuery();
    }
    catch (...)
    {
        /// Some databases may not support getCreateDatabaseQuery (e.g. temporary databases).
        /// Just skip them.
        tryLogCurrentException(log, fmt::format("Cannot get CREATE DATABASE query for '{}'", database->getDatabaseName()));
        throw;
    }

    if (!create_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get CREATE DATABASE query for '{}'", database->getDatabaseName());

    /// Convert CREATE to ATTACH to match the on-disk metadata file format.
    auto * ast_create = create_query->as<ASTCreateQuery>();
    ast_create->attach = true;
    ast_create->if_not_exists = false;
    WriteBufferFromOwnString statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false);
    ast_create->format(statement_buf, format_settings);
    writeChar('\n', statement_buf);
    return statement_buf.str();
}

String DatabaseReplicator::getCreateDatabaseStatement(const String & database_name)
{
    auto database = DatabaseCatalog::instance().getDatabase(database_name);
    return getCreateDatabaseStatement(database);
}

ASTPtr DatabaseReplicator::parseQueryFromMetadataInZooKeeper(ContextPtr context_, const String & database_name, const String & query) const
{
    String description = fmt::format("in ZooKeeper {}/metadata/{}", zookeeper_path, escapeForFileName(database_name));
    ParserCreateQuery parser;
    auto ast = parseQuery(
        parser,
        query,
        description,
        0,
        context_->getSettingsRef()[Setting::max_parser_depth],
        context_->getSettingsRef()[Setting::max_parser_backtracks]);
    auto & create = ast->as<ASTCreateQuery &>();
    if (create.uuid == UUIDHelpers::Nil || create.table || !create.database)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected query from {}: {}", database_name, query);
    create.setDatabase(database_name);
    create.attach = false;
    return ast;
}

void DatabaseReplicator::recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 & max_log_ptr)
{
    is_recovering = true;
    SCOPE_EXIT({ is_recovering = false; });

    /// Let's compare local (possibly outdated) metadata with (most actual) metadata stored in ZooKeeper
    /// and try to update the set of local tables.
    bool new_replica = our_log_ptr == 0;
    if (new_replica)
        LOG_INFO(log, "Will create new replica from log pointer {}", max_log_ptr);
    else
        LOG_WARNING(log, "Will recover replica with staled log pointer {} from log pointer {}", our_log_ptr, max_log_ptr);

    auto database_name_to_metadata = tryGetConsistentMetadataSnapshot(current_zookeeper, max_log_ptr);

    /// We will drop or move databases which exist only in local metadata
    Strings databases_to_detach;
    std::map<String, String> databases_to_create;
    UInt64 new_digest = 0;
    std::map<String, UInt64> new_local_digests;
    {
        std::lock_guard lock{metadata_mutex};
        new_digest = metadata_digest;
        new_local_digests = local_digests;
        for (const auto & [database_name, digest] : local_digests)
        {
            LOG_TEST(log, "Existing database {}", database_name);
            auto in_zk = database_name_to_metadata.find(database_name);
            if (in_zk == database_name_to_metadata.end() || getMetadataHash(in_zk->first, in_zk->second) != digest)
                databases_to_detach.emplace_back(database_name);
        }
        for (const auto & [database_name, metadata] : database_name_to_metadata)
        {
            if (!local_digests.contains(database_name))
            {
                auto digest = getMetadataHash(database_name, metadata);
                new_digest += digest;
                new_local_digests[database_name] = digest;
                databases_to_create[database_name] = metadata;
            }
        }
    }

    /// For safety, we will not drop any databases automatically but throw an exception for user.
    if (!databases_to_detach.empty())
    {
        /// TODO: try deal with the databases automatically.
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Local databases ([{}]) are different from the metadata in ZooKeeper, should repair them manually",
            fmt::join(databases_to_detach, ", "));
    }

    auto make_query_context = [this, current_zookeeper]()
    {
        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();
        query_context->setQueryKind(ClientInfo::QueryKind::SECONDARY_QUERY);
        query_context->setQueryKindReplicatedDatabaseInternal();
        query_context->setCurrentQueryId({});

        /// We will execute some CREATE queries for recovery (not ATTACH queries),
        /// so we need to allow experimental features that can be used in a CREATE query
        enableAllExperimentalSettings(query_context);

        query_context->setSetting("database_replicated_allow_explicit_uuid", 3);
        query_context->setSetting("database_replicated_allow_replicated_engine_arguments", 3);

        /// We apply the flatten_nested setting after writing the CREATE query to the DDL log,
        /// but before writing metadata to ZooKeeper. So we have to apply the setting on secondary replicas, but not in recovery mode.
        /// Set it to false, so it will do nothing on recovery. The metadata in ZooKeeper should be used as is.
        /// Same for data_type_default_nullable.
        query_context->setSetting("flatten_nested", false);
        query_context->setSetting("data_type_default_nullable", false);

        auto txn = std::make_shared<ZooKeeperMetadataTransaction>(current_zookeeper, zookeeper_path, false, "");
        query_context->initZooKeeperMetadataTransaction(txn);
        return query_context;
    };

    LOG_TEST(log, "table_name_to_metadata size={}", database_name_to_metadata.size());

    for (const auto & [database_name, create_query_sql] : databases_to_create)
    {
        auto create_query = parseQueryFromMetadataInZooKeeper(getContext(), database_name, create_query_sql);
        auto query_context = make_query_context();
        InterpreterCreateQuery(create_query, query_context).execute();
    }
    LOG_INFO(log, "All databases are created successfully");

    UInt32 first_entry_to_mark_finished = new_replica ? max_log_ptr_at_creation : our_log_ptr;
    /// NOTE first_entry_to_mark_finished can be 0 if our replica has crashed just after creating its nodes in ZK,
    /// so it's a new replica, but after restarting we don't know max_log_ptr_at_creation anymore...
    /// It's a very rare case, and it's okay if some queries throw TIMEOUT_EXCEEDED when waiting for all replicas
    if (first_entry_to_mark_finished)
    {
        /// Skip non-existing entries that were removed a long time ago (if the replica was offline for a long time)
        Strings all_nodes = current_zookeeper->getChildren(fs::path(zookeeper_path) / "log");
        std::erase_if(all_nodes, [] (const String & s) { return !startsWith(s, "query-"); });
        auto oldest_node = std::min_element(all_nodes.begin(), all_nodes.end());
        if (oldest_node != all_nodes.end())
        {
            UInt32 oldest_entry = DDLTaskBase::getLogEntryNumber(*oldest_node);
            first_entry_to_mark_finished = std::max(oldest_entry, first_entry_to_mark_finished);
        }

        /// If the replica is new and some of the queries applied during recovery
        /// where issued after the replica was created, then other nodes might be
        /// waiting for this node to notify them that the query was applied.
        for (UInt32 ptr = first_entry_to_mark_finished; ptr <= max_log_ptr; ++ptr)
        {
            auto entry_name = DDLTaskBase::getLogEntryName(ptr);

            auto finished = fs::path(zookeeper_path) / "log" / entry_name / "finished" / getFullReplicaName();
            auto synced = fs::path(zookeeper_path) / "log" / entry_name / "synced" / getFullReplicaName();

            auto status = ExecutionStatus(0).serializeText();
            auto res_finished = current_zookeeper->tryCreate(finished, status, zkutil::CreateMode::Persistent);
            auto res_synced = current_zookeeper->tryCreate(synced, status, zkutil::CreateMode::Persistent);
            if (res_finished == Coordination::Error::ZOK && res_synced == Coordination::Error::ZOK)
                LOG_INFO(log, "Marked recovered {} as finished", entry_name);
            else
                LOG_INFO(log, "Failed to marked {} as finished (finished={}, synced={}). Ignoring.", entry_name, res_finished, res_synced);
        }
    }

    std::lock_guard lock{metadata_mutex};
    local_digests = std::move(new_local_digests);
    metadata_digest = new_digest;
    assertDigest(getContext());
    current_zookeeper->set(replica_path + "/digest", toString(metadata_digest));
}

}
