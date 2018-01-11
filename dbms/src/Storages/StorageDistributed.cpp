#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/BlockExtraInfoInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>

#include <Databases/IDatabase.h>

#include <Storages/StorageDistributed.h>
#include <Storages/VirtualColumnFactory.h>
#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/MergeTree/ReshardingWorker.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTWeightedZooKeeperPath.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/queryToString.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/DescribeStreamFactory.h>
#include <Interpreters/ClusterProxy/AlterStreamFactory.h>

#include <Core/Field.h>

#include <Poco/DirectoryIterator.h>

#include <memory>

#include <boost/filesystem.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int STORAGE_REQUIRES_PARAMETER;
    extern const int RESHARDING_NO_WORKER;
    extern const int RESHARDING_INVALID_PARAMETERS;
    extern const int RESHARDING_INITIATOR_CHECK_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int READONLY;
}


namespace
{

/// select query has database and table names as AST pointers
/// Creates a copy of query, changes database and table names.
ASTPtr rewriteSelectQuery(const ASTPtr & query, const std::string & database, const std::string & table)
{
    auto modified_query_ast = typeid_cast<const ASTSelectQuery &>(*query).cloneFirstSelect();
    modified_query_ast->replaceDatabaseAndTable(database, table);
    return modified_query_ast;
}

/// insert query has database and table names as bare strings
/// Creates a copy of query, changes the database and table names.
ASTPtr rewriteInsertQuery(const ASTPtr & query, const std::string & database, const std::string & table)
{
    auto modified_query_ast = query->clone();

    auto & actual_query = typeid_cast<ASTInsertQuery &>(*modified_query_ast);
    actual_query.database = database;
    actual_query.table = table;
    /// make sure query is not INSERT SELECT
    actual_query.select = nullptr;

    return modified_query_ast;
}

/// Calculate maximum number in file names in directory and all subdirectories.
/// To ensure global order of data blocks yet to be sent across server restarts.
UInt64 getMaximumFileNumber(const std::string & path)
{
    UInt64 res = 0;

    boost::filesystem::recursive_directory_iterator begin(path);
    boost::filesystem::recursive_directory_iterator end;
    for (auto it = begin; it != end; ++it)
    {
        const auto & path = it->path();

        if (it->status().type() != boost::filesystem::regular_file || !endsWith(path.filename().string(), ".bin"))
            continue;

        UInt64 num = 0;
        try
        {
            num = parse<UInt64>(path.filename().stem().string());
        }
        catch (Exception & e)
        {
            e.addMessage("Unexpected file name " + path.filename().string() + " found at " + path.parent_path().string() + ", should have numeric base name.");
            throw;
        }

        if (num > res)
            res = num;
    }

    return res;
}

void initializeFileNamesIncrement(const std::string & path, SimpleIncrement & increment)
{
    if (!path.empty())
        increment.set(getMaximumFileNumber(path));
}

}


/// For destruction of std::unique_ptr of type that is incomplete in class definition.
StorageDistributed::~StorageDistributed() = default;


StorageDistributed::StorageDistributed(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const String & remote_database_,
    const String & remote_table_,
    const String & cluster_name_,
    const Context & context_,
    const ASTPtr & sharding_key_,
    const String & data_path_)
    : name(name_), columns(columns_),
    remote_database(remote_database_), remote_table(remote_table_),
    context(context_), cluster_name(cluster_name_), has_sharding_key(sharding_key_),
    sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, nullptr, *columns).getActions(false) : nullptr),
    sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
    path(data_path_.empty() ? "" : (data_path_ + escapeForFileName(name) + '/'))
{
}


StorageDistributed::StorageDistributed(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const String & remote_database_,
    const String & remote_table_,
    const String & cluster_name_,
    const Context & context_,
    const ASTPtr & sharding_key_,
    const String & data_path_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
    name(name_), columns(columns_),
    remote_database(remote_database_), remote_table(remote_table_),
    context(context_), cluster_name(cluster_name_), has_sharding_key(sharding_key_),
    sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, nullptr, *columns).getActions(false) : nullptr),
    sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
    path(data_path_.empty() ? "" : (data_path_ + escapeForFileName(name) + '/'))
{
}


StoragePtr StorageDistributed::createWithOwnCluster(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const String & remote_database_,
    const String & remote_table_,
    ClusterPtr & owned_cluster_,
    const Context & context_)
{
    auto res = make_shared(
        name_, columns_, remote_database_,
        remote_table_, String{}, context_);

    res->owned_cluster = owned_cluster_;

    return res;
}


BlockInputStreams StorageDistributed::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    auto cluster = getCluster();

    const Settings & settings = context.getSettingsRef();

    size_t result_size = (cluster->getRemoteShardCount() * settings.max_parallel_replicas) + cluster->getLocalShardCount();

    processed_stage = result_size == 1 || settings.distributed_group_by_no_merge
        ? QueryProcessingStage::Complete
        : QueryProcessingStage::WithMergeableState;

    const auto & modified_query_ast = rewriteSelectQuery(
        query_info.query, remote_database, remote_table);

    Tables external_tables;

    if (settings.global_subqueries_method == GlobalSubqueriesMethod::PUSH)
        external_tables = context.getExternalTables();

    ClusterProxy::SelectStreamFactory select_stream_factory(
        processed_stage, QualifiedTableName{remote_database, remote_table}, external_tables);

    return ClusterProxy::executeQuery(
            select_stream_factory, cluster, modified_query_ast, context, settings);
}


BlockOutputStreamPtr StorageDistributed::write(const ASTPtr & query, const Settings & settings)
{
    if (owned_cluster && context.getApplicationType() != Context::ApplicationType::LOCAL)
        throw Exception(
            "Method write is not supported by storage " + getName() +
            " created via a table function", ErrorCodes::READONLY);

    auto cluster = (owned_cluster) ? owned_cluster : context.getCluster(cluster_name);

    bool is_sharding_key_ok = has_sharding_key || ((cluster->getLocalShardCount() + cluster->getRemoteShardCount()) < 2);
    if (!is_sharding_key_ok)
        throw Exception(
            "Method write is not supported by storage " + getName() +
            " with more than one shard and no sharding key provided",
            ErrorCodes::STORAGE_REQUIRES_PARAMETER);

    if (path.empty() && !settings.insert_distributed_sync.value)
        throw Exception(
            "Data path should be set for storage " + getName() +
            " to enable asynchronous inserts", ErrorCodes::BAD_ARGUMENTS);

    /// DistributedBlockOutputStream will not own cluster, but will own ConnectionPools of the cluster
    return std::make_shared<DistributedBlockOutputStream>(
        *this, rewriteInsertQuery(query, remote_database, remote_table), cluster, settings.insert_distributed_sync, settings.insert_distributed_timeout);
}


void StorageDistributed::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
    for (const auto & param : params)
        if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
            throw Exception("Storage engine " + getName() + " doesn't support primary key.", ErrorCodes::NOT_IMPLEMENTED);

    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);
    params.apply(*columns, materialized_columns, alias_columns, column_defaults);

    context.getDatabase(database_name)->alterTable(
        context, table_name,
        *columns, materialized_columns, alias_columns, column_defaults, {});
}


void StorageDistributed::startup()
{
    createDirectoryMonitors();
    initializeFileNamesIncrement(path, file_names_increment);
}


void StorageDistributed::shutdown()
{
    cluster_nodes_data.clear();
}


void StorageDistributed::reshardPartitions(
    const ASTPtr & query, const String & database_name,
    const ASTPtr & partition,
    const WeightedZooKeeperPaths & weighted_zookeeper_paths,
    const ASTPtr & sharding_key_expr, bool do_copy, const Field & coordinator,
    const Context & context)
{
    auto & resharding_worker = context.getReshardingWorker();
    if (!resharding_worker.isStarted())
        throw Exception{"Resharding background thread is not running", ErrorCodes::RESHARDING_NO_WORKER};

    if (!coordinator.isNull())
        throw Exception{"Use of COORDINATE WITH is forbidden in ALTER TABLE ... RESHARD"
            " queries for distributed tables",
            ErrorCodes::RESHARDING_INVALID_PARAMETERS};

    auto cluster = getCluster();

    /// resharding_worker doesn't need to own cluster, here only meta-information of cluster is used
    std::string coordinator_id = resharding_worker.createCoordinator(*cluster);

    std::atomic<bool> has_notified_error{false};

    std::string dumped_coordinator_state;

    auto handle_exception = [&](const std::string & msg = "")
    {
        try
        {
            if (!has_notified_error)
                resharding_worker.setStatus(coordinator_id, ReshardingWorker::STATUS_ERROR, msg);
            dumped_coordinator_state = resharding_worker.dumpCoordinatorState(coordinator_id);
            resharding_worker.deleteCoordinator(coordinator_id);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    };

    try
    {
        /// Create query ALTER TABLE ... RESHARD [COPY] PARTITION ... COORDINATE WITH ...

        ASTPtr alter_query_ptr = std::make_shared<ASTAlterQuery>();
        auto & alter_query = static_cast<ASTAlterQuery &>(*alter_query_ptr);

        alter_query.database = remote_database;
        alter_query.table = remote_table;

        alter_query.parameters.emplace_back();
        ASTAlterQuery::Parameters & parameters = alter_query.parameters.back();

        parameters.type = ASTAlterQuery::RESHARD_PARTITION;
        if (partition)
            parameters.partition = partition->clone();

        ASTPtr expr_list = std::make_shared<ASTExpressionList>();
        for (const auto & entry : weighted_zookeeper_paths)
        {
            ASTPtr weighted_path_ptr = std::make_shared<ASTWeightedZooKeeperPath>();
            auto & weighted_path = static_cast<ASTWeightedZooKeeperPath &>(*weighted_path_ptr);
            weighted_path.path = entry.first;
            weighted_path.weight = entry.second;
            expr_list->children.push_back(weighted_path_ptr);
        }

        parameters.weighted_zookeeper_paths = expr_list;
        parameters.sharding_key_expr = sharding_key_expr;
        parameters.do_copy = do_copy;
        parameters.coordinator = std::make_shared<ASTLiteral>(StringRange(), Field(coordinator_id));

        resharding_worker.registerQuery(coordinator_id, queryToString(alter_query_ptr));

        ClusterProxy::AlterStreamFactory alter_stream_factory;

        BlockInputStreams streams = ClusterProxy::executeQuery(
                alter_stream_factory, cluster, alter_query_ptr, context, context.getSettingsRef());

        /// This callback is called if an exception has occurred while attempting to read
        /// a block from a shard. This is to avoid a potential deadlock if other shards are
        /// waiting inside a barrier. Actually, even without this solution, we would avoid
        /// such a deadlock because we would eventually time out while trying to get remote
        /// blocks. Nevertheless this is not the ideal way of sorting out this issue since
        /// we would then not get to know the actual cause of the failure.
        auto exception_callback = [&resharding_worker, coordinator_id, &has_notified_error]()
        {
            try
            {
                resharding_worker.setStatus(coordinator_id, ReshardingWorker::STATUS_ERROR);
                has_notified_error = true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        };

        streams[0] = std::make_shared<UnionBlockInputStream<>>(
            streams, nullptr, context.getSettingsRef().max_distributed_connections, exception_callback);
        streams.resize(1);

        auto stream_ptr = dynamic_cast<IProfilingBlockInputStream *>(&*streams[0]);
        if (stream_ptr == nullptr)
            throw Exception{"StorageDistributed: Internal error", ErrorCodes::LOGICAL_ERROR};
        auto & stream = *stream_ptr;

        stream.readPrefix();

        while (!stream.isCancelled() && stream.read())
            ;

        if (!stream.isCancelled())
            stream.readSuffix();
    }
    catch (const Exception & ex)
    {
        handle_exception(ex.message());
        LOG_ERROR(log, dumped_coordinator_state);
        throw;
    }
    catch (const std::exception & ex)
    {
        handle_exception(ex.what());
        LOG_ERROR(log, dumped_coordinator_state);
        throw;
    }
    catch (...)
    {
        handle_exception();
        LOG_ERROR(log, dumped_coordinator_state);
        throw;
    }
}


NameAndTypePair StorageDistributed::getColumn(const String & column_name) const
{
    if (const auto & type = VirtualColumnFactory::tryGetType(column_name))
        return { column_name, type };

    return getRealColumn(column_name);
}


bool StorageDistributed::hasColumn(const String & column_name) const
{
    return VirtualColumnFactory::hasColumn(column_name) || IStorage::hasColumn(column_name);
}

void StorageDistributed::createDirectoryMonitors()
{
    if (path.empty())
        return;

    Poco::File{path}.createDirectory();

    boost::filesystem::directory_iterator begin(path);
    boost::filesystem::directory_iterator end;
    for (auto it = begin; it != end; ++it)
        if (it->status().type() == boost::filesystem::directory_file)
            requireDirectoryMonitor(it->path().filename().string());
}


void StorageDistributed::requireDirectoryMonitor(const std::string & name)
{
    cluster_nodes_data[name].requireDirectoryMonitor(name, *this);
}

ConnectionPoolPtr StorageDistributed::requireConnectionPool(const std::string & name)
{
    auto & node_data = cluster_nodes_data[name];
    node_data.requireConnectionPool(name, *this);
    return node_data.conneciton_pool;
}

size_t StorageDistributed::getShardCount() const
{
    return getCluster()->getRemoteShardCount();
}


ClusterPtr StorageDistributed::getCluster() const
{
    return (owned_cluster) ? owned_cluster : context.getCluster(cluster_name);
}

void StorageDistributed::ClusterNodeData::requireConnectionPool(const std::string & name, const StorageDistributed & storage)
{
    if (!conneciton_pool)
        conneciton_pool = StorageDistributedDirectoryMonitor::createPool(name, storage);
}

void StorageDistributed::ClusterNodeData::requireDirectoryMonitor(const std::string & name, StorageDistributed & storage)
{
    requireConnectionPool(name, storage);
    if (!directory_monitor)
        directory_monitor = std::make_unique<StorageDistributedDirectoryMonitor>(storage, name, conneciton_pool);
}

}
