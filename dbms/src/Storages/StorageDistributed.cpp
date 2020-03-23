#include <Storages/StorageDistributed.h>

#include <DataStreams/OneBlockInputStream.h>

#include <Databases/IDatabase.h>
#include <Disks/DiskSpaceMonitor.h>
#include <Disks/DiskLocal.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>

#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>

#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>

#include <Core/Field.h>

#include <IO/ReadHelpers.h>

#include <Poco/DirectoryIterator.h>

#include <memory>
#include <filesystem>


namespace
{
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY = 1;
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS           = 2;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int STORAGE_REQUIRES_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INFINITE_LOOP;
    extern const int TYPE_MISMATCH;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int TOO_MANY_ROWS;
    extern const int UNABLE_TO_SKIP_UNUSED_SHARDS;
}

namespace ActionLocks
{
    extern const StorageActionBlockType DistributedSend;
}

namespace
{

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
ASTPtr rewriteSelectQuery(const ASTPtr & query, const std::string & database, const std::string & table, ASTPtr table_function_ptr = nullptr)
{
    auto modified_query_ast = query->clone();

    ASTSelectQuery & select_query = modified_query_ast->as<ASTSelectQuery &>();

    /// restore long column names in JOIN ON expressions
    if (auto tables = select_query.tables())
    {
        RestoreQualifiedNamesVisitor::Data data;
        RestoreQualifiedNamesVisitor(data).visit(tables);
    }

    if (table_function_ptr)
        select_query.addTableFunction(table_function_ptr);
    else
        select_query.replaceDatabaseAndTable(database, table);
    return modified_query_ast;
}

/// The columns list in the original INSERT query is incorrect because inserted blocks are transformed
/// to the form of the sample block of the Distributed table. So we rewrite it and add all columns from
/// the sample block instead.
ASTPtr createInsertToRemoteTableQuery(const std::string & database, const std::string & table, const Block & sample_block_non_materialized)
{
    auto query = std::make_shared<ASTInsertQuery>();
    query->table_id = StorageID(database, table);

    auto columns = std::make_shared<ASTExpressionList>();
    query->columns = columns;
    query->children.push_back(columns);
    for (const auto & col : sample_block_non_materialized)
        columns->children.push_back(std::make_shared<ASTIdentifier>(col.name));

    return query;
}

/// Calculate maximum number in file names in directory and all subdirectories.
/// To ensure global order of data blocks yet to be sent across server restarts.
UInt64 getMaximumFileNumber(const std::string & dir_path)
{
    UInt64 res = 0;

    std::filesystem::recursive_directory_iterator begin(dir_path);
    std::filesystem::recursive_directory_iterator end;
    for (auto it = begin; it != end; ++it)
    {
        const auto & file_path = it->path();

        if (!std::filesystem::is_regular_file(*it) || !endsWith(file_path.filename().string(), ".bin"))
            continue;

        UInt64 num = 0;
        try
        {
            num = parse<UInt64>(file_path.filename().stem().string());
        }
        catch (Exception & e)
        {
            e.addMessage("Unexpected file name " + file_path.filename().string() + " found at " + file_path.parent_path().string() + ", should have numeric base name.");
            throw;
        }

        if (num > res)
            res = num;
    }

    return res;
}

/// the same as DistributedBlockOutputStream::createSelector, should it be static?
IColumn::Selector createSelector(const ClusterPtr cluster, const ColumnWithTypeAndName & result)
{
    const auto & slot_to_shard = cluster->getSlotToShard();

#define CREATE_FOR_TYPE(TYPE)                                   \
    if (typeid_cast<const DataType##TYPE *>(result.type.get())) \
        return createBlockSelector<TYPE>(*result.column, slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}

std::string makeFormattedListOfShards(const ClusterPtr & cluster)
{
    std::ostringstream os;

    bool head = true;
    os << "[";
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        (head ? os : os << ", ") << shard_info.shard_num;
        head = false;
    }
    os << "]";

    return os.str();
}

}


/// For destruction of std::unique_ptr of type that is incomplete in class definition.
StorageDistributed::~StorageDistributed() = default;

static ExpressionActionsPtr buildShardingKeyExpression(const ASTPtr & sharding_key, const Context & context, NamesAndTypesList columns, bool project)
{
    ASTPtr query = sharding_key;
    auto syntax_result = SyntaxAnalyzer(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
}


StorageDistributed::StorageDistributed(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & remote_database_,
    const String & remote_table_,
    const String & cluster_name_,
    const Context & context_,
    const ASTPtr & sharding_key_,
    const String & storage_policy_,
    const String & relative_data_path_,
    bool attach_)
    : IStorage(id_,
               ColumnsDescription(
                   {
                       {"_shard_num", std::make_shared<DataTypeUInt32>()},
                   },
               true))
    , remote_database(remote_database_)
    , remote_table(remote_table_)
    , global_context(context_)
    , cluster_name(global_context.getMacros()->expand(cluster_name_))
    , has_sharding_key(sharding_key_)
    , storage_policy(storage_policy_)
    , relative_data_path(relative_data_path_)
{
    setColumns(columns_);
    setConstraints(constraints_);

    if (sharding_key_)
    {
        sharding_key_expr = buildShardingKeyExpression(sharding_key_, global_context, getColumns().getAllPhysical(), false);
        sharding_key_column_name = sharding_key_->getColumnName();
    }

    if (!relative_data_path.empty())
        createStorage();

    /// Sanity check. Skip check if the table is already created to allow the server to start.
    if (!attach_ && !cluster_name.empty())
    {
        size_t num_local_shards = global_context.getCluster(cluster_name)->getLocalShardCount();
        if (num_local_shards && remote_database == id_.database_name && remote_table == id_.table_name)
            throw Exception("Distributed table " + id_.table_name + " looks at itself", ErrorCodes::INFINITE_LOOP);
    }
}


StorageDistributed::StorageDistributed(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ASTPtr remote_table_function_ptr_,
    const String & cluster_name_,
    const Context & context_,
    const ASTPtr & sharding_key_,
    const String & storage_policy_,
    const String & relative_data_path_,
    bool attach)
    : StorageDistributed(id_, columns_, constraints_, String{}, String{}, cluster_name_, context_, sharding_key_, storage_policy_, relative_data_path_, attach)
{
    remote_table_function_ptr = std::move(remote_table_function_ptr_);
}

void StorageDistributed::createStorage()
{
    /// Create default policy with the relative_data_path_
    if (storage_policy.empty())
    {
        std::string path(global_context.getPath());
        /// Disk must ends with '/'
        if (!path.ends_with('/'))
            path += '/';
        auto disk = std::make_shared<DiskLocal>("default", path, 0);
        volume = std::make_shared<Volume>("default", std::vector<DiskPtr>{disk}, 0);
    }
    else
    {
        auto policy = global_context.getStoragePolicySelector()->get(storage_policy);
        if (policy->getVolumes().size() != 1)
             throw Exception("Policy for Distributed table, should have exactly one volume", ErrorCodes::BAD_ARGUMENTS);
        volume = policy->getVolume(0);
    }
}

StoragePtr StorageDistributed::createWithOwnCluster(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & remote_database_,       /// database on remote servers.
    const String & remote_table_,          /// The name of the table on the remote servers.
    ClusterPtr owned_cluster_,
    const Context & context_)
{
    auto res = create(table_id_, columns_, ConstraintsDescription{}, remote_database_, remote_table_, String{}, context_, ASTPtr(), String(), String(), false);
    res->owned_cluster = std::move(owned_cluster_);
    return res;
}


StoragePtr StorageDistributed::createWithOwnCluster(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    ASTPtr & remote_table_function_ptr_,
    ClusterPtr & owned_cluster_,
    const Context & context_)
{
    auto res = create(table_id_, columns_, ConstraintsDescription{}, remote_table_function_ptr_, String{}, context_, ASTPtr(), String(), String(), false);
    res->owned_cluster = owned_cluster_;
    return res;
}


static QueryProcessingStage::Enum getQueryProcessingStageImpl(const Context & context, const ClusterPtr & cluster)
{
    const Settings & settings = context.getSettingsRef();

    size_t num_local_shards = cluster->getLocalShardCount();
    size_t num_remote_shards = cluster->getRemoteShardCount();
    size_t result_size = (num_remote_shards * settings.max_parallel_replicas) + num_local_shards;

    if (settings.distributed_group_by_no_merge)
        return QueryProcessingStage::Complete;
    else    /// Normal mode.
        return result_size == 1 ? QueryProcessingStage::Complete
                                : QueryProcessingStage::WithMergeableState;
}

QueryProcessingStage::Enum StorageDistributed::getQueryProcessingStage(const Context & context) const
{
    auto cluster = getCluster();
    return getQueryProcessingStageImpl(context, cluster);
}

Pipes StorageDistributed::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    auto cluster = getCluster();

    const Settings & settings = context.getSettingsRef();

    const auto & modified_query_ast = rewriteSelectQuery(
        query_info.query, remote_database, remote_table, remote_table_function_ptr);

    Block header =
        InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage)).getSampleBlock();

    const Scalars & scalars = context.hasQueryContext() ? context.getQueryContext().getScalars() : Scalars{};

    bool has_virtual_shard_num_column = std::find(column_names.begin(), column_names.end(), "_shard_num") != column_names.end();
    if (has_virtual_shard_num_column && !isVirtualColumn("_shard_num"))
        has_virtual_shard_num_column = false;

    ClusterProxy::SelectStreamFactory select_stream_factory = remote_table_function_ptr
        ? ClusterProxy::SelectStreamFactory(
            header, processed_stage, remote_table_function_ptr, scalars, has_virtual_shard_num_column, context.getExternalTables())
        : ClusterProxy::SelectStreamFactory(
            header, processed_stage, StorageID{remote_database, remote_table}, scalars, has_virtual_shard_num_column, context.getExternalTables());

    UInt64 force = settings.force_optimize_skip_unused_shards;
    if (settings.optimize_skip_unused_shards)
    {
        ClusterPtr smaller_cluster;
        auto table_id = getStorageID();

        if (has_sharding_key)
        {
            smaller_cluster = skipUnusedShards(cluster, query_info);

            if (smaller_cluster)
            {
                cluster = smaller_cluster;
                LOG_DEBUG(log, "Reading from " << table_id.getNameForLogs() << ": "
                               "Skipping irrelevant shards - the query will be sent to the following shards of the cluster (shard numbers): "
                               " " << makeFormattedListOfShards(cluster));
            }
        }

        if (!smaller_cluster)
        {
            LOG_DEBUG(log, "Reading from " << table_id.getNameForLogs() <<
                           (has_sharding_key ? "" : "(no sharding key)") << ": "
                           "Unable to figure out irrelevant shards from WHERE/PREWHERE clauses - "
                           "the query will be sent to all shards of the cluster");

            if (force)
            {
                std::stringstream exception_message;
                if (!has_sharding_key)
                    exception_message << "No sharding key";
                else
                    exception_message << "Sharding key " << sharding_key_column_name << " is not used";

                if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS)
                    throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
                if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY && has_sharding_key)
                    throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
            }
        }
    }

    return ClusterProxy::executeQuery(
        select_stream_factory, cluster, modified_query_ast, context, settings, query_info);
}


BlockOutputStreamPtr StorageDistributed::write(const ASTPtr &, const Context & context)
{
    auto cluster = getCluster();
    const auto & settings = context.getSettingsRef();

    /// Ban an attempt to make async insert into the table belonging to DatabaseMemory
    if (!volume && !owned_cluster && !settings.insert_distributed_sync)
    {
        throw Exception("Storage " + getName() + " must has own data directory to enable asynchronous inserts",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    /// If sharding key is not specified, then you can only write to a shard containing only one shard
    if (!has_sharding_key && ((cluster->getLocalShardCount() + cluster->getRemoteShardCount()) >= 2))
    {
        throw Exception("Method write is not supported by storage " + getName() + " with more than one shard and no sharding key provided",
                        ErrorCodes::STORAGE_REQUIRES_PARAMETER);
    }

    /// Force sync insertion if it is remote() table function
    bool insert_sync = settings.insert_distributed_sync || owned_cluster;
    auto timeout = settings.insert_distributed_timeout;

    /// DistributedBlockOutputStream will not own cluster, but will own ConnectionPools of the cluster
    return std::make_shared<DistributedBlockOutputStream>(
        context, *this, createInsertToRemoteTableQuery(remote_database, remote_table, getSampleBlockNonMaterialized()), cluster,
        insert_sync, timeout);
}


void StorageDistributed::checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */)
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN
            && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN
            && command.type != AlterCommand::Type::COMMENT_COLUMN)

            throw Exception("Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
    }
}

void StorageDistributed::alter(const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder)
{
    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());
    auto table_id = getStorageID();

    checkAlterIsPossible(params, context.getSettingsRef());
    StorageInMemoryMetadata metadata = getInMemoryMetadata();
    params.apply(metadata);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id.table_name, metadata);
    setColumns(std::move(metadata.columns));
}


void StorageDistributed::startup()
{
    if (remote_database.empty() && !remote_table_function_ptr)
        LOG_WARNING(log, "Name of remote database is empty. Default database will be used implicitly.");

    if (!volume)
        return;

    for (const DiskPtr & disk : volume->disks)
        createDirectoryMonitors(disk->getPath());

    for (const String & path : getDataPaths())
    {
        UInt64 inc = getMaximumFileNumber(path);
        if (inc > file_names_increment.value)
            file_names_increment.value.store(inc);
    }
    LOG_DEBUG(log, "Auto-increment is " << file_names_increment.value);
}


void StorageDistributed::shutdown()
{
    cluster_nodes_data.clear();
}

Strings StorageDistributed::getDataPaths() const
{
    Strings paths;

    if (relative_data_path.empty())
        return paths;

    for (const DiskPtr & disk : volume->disks)
        paths.push_back(disk->getPath() + relative_data_path);

    return paths;
}

void StorageDistributed::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
{
    std::lock_guard lock(cluster_nodes_mutex);

    for (auto it = cluster_nodes_data.begin(); it != cluster_nodes_data.end();)
    {
        it->second.shutdownAndDropAllData();
        it = cluster_nodes_data.erase(it);
    }
}


namespace
{
    /// NOTE This is weird. Get rid of this.
    std::map<String, String> virtual_columns =
    {
        {"_table", "String"},
        {"_part", "String"},
        {"_part_index", "UInt64"},
        {"_partition_id", "String"},
        {"_sample_factor", "Float64"},
    };
}


NameAndTypePair StorageDistributed::getColumn(const String & column_name) const
{
    if (getColumns().hasPhysical(column_name))
        return getColumns().getPhysical(column_name);

    auto it = virtual_columns.find(column_name);
    if (it != virtual_columns.end())
        return { it->first, DataTypeFactory::instance().get(it->second) };

    throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
}


bool StorageDistributed::hasColumn(const String & column_name) const
{
    return virtual_columns.count(column_name) || getColumns().hasPhysical(column_name);
}

void StorageDistributed::createDirectoryMonitors(const std::string & disk)
{
    const std::string path(disk + relative_data_path);
    Poco::File{path}.createDirectories();

    std::filesystem::directory_iterator begin(path);
    std::filesystem::directory_iterator end;
    for (auto it = begin; it != end; ++it)
        if (std::filesystem::is_directory(*it))
            requireDirectoryMonitor(disk, it->path().filename().string());
}


void StorageDistributed::requireDirectoryMonitor(const std::string & disk, const std::string & name)
{
    const std::string path(disk + relative_data_path + name);
    const std::string key(disk + name);

    std::lock_guard lock(cluster_nodes_mutex);
    auto & node_data = cluster_nodes_data[key];
    node_data.conneciton_pool = StorageDistributedDirectoryMonitor::createPool(name, *this);
    node_data.directory_monitor = std::make_unique<StorageDistributedDirectoryMonitor>(*this, path, node_data.conneciton_pool, monitors_blocker);
}

size_t StorageDistributed::getShardCount() const
{
    return getCluster()->getShardCount();
}

std::pair<const std::string &, const std::string &> StorageDistributed::getPath()
{
    return {volume->getNextDisk()->getPath(), relative_data_path};
}

ClusterPtr StorageDistributed::getCluster() const
{
    return owned_cluster ? owned_cluster : global_context.getCluster(cluster_name);
}

void StorageDistributed::ClusterNodeData::flushAllData()
{
    directory_monitor->flushAllData();
}

void StorageDistributed::ClusterNodeData::shutdownAndDropAllData()
{
    directory_monitor->shutdownAndDropAllData();
}

/// Returns a new cluster with fewer shards if constant folding for `sharding_key_expr` is possible
/// using constraints from "PREWHERE" and "WHERE" conditions, otherwise returns `nullptr`
ClusterPtr StorageDistributed::skipUnusedShards(ClusterPtr cluster, const SelectQueryInfo & query_info)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();

    if (!select.prewhere() && !select.where())
    {
        return nullptr;
    }

    ASTPtr condition_ast;
    if (select.prewhere() && select.where())
    {
        condition_ast = makeASTFunction("and", select.prewhere()->clone(), select.where()->clone());
    }
    else
    {
        condition_ast = select.prewhere() ? select.prewhere()->clone() : select.where()->clone();
    }

    const auto blocks = evaluateExpressionOverConstantCondition(condition_ast, sharding_key_expr);

    // Can't get definite answer if we can skip any shards
    if (!blocks)
    {
        return nullptr;
    }

    std::set<int> shards;

    for (const auto & block : *blocks)
    {
        if (!block.has(sharding_key_column_name))
            throw Exception("sharding_key_expr should evaluate as a single row", ErrorCodes::TOO_MANY_ROWS);

        const ColumnWithTypeAndName & result = block.getByName(sharding_key_column_name);
        const auto selector = createSelector(cluster, result);

        shards.insert(selector.begin(), selector.end());
    }

    return cluster->getClusterWithMultipleShards({shards.begin(), shards.end()});
}

ActionLock StorageDistributed::getActionLock(StorageActionBlockType type)
{
    if (type == ActionLocks::DistributedSend)
        return monitors_blocker.cancel();
    return {};
}

void StorageDistributed::flushClusterNodesAllData()
{
    std::lock_guard lock(cluster_nodes_mutex);

    /// TODO: Maybe it should be executed in parallel
    for (auto & node : cluster_nodes_data)
        node.second.flushAllData();
}

void StorageDistributed::rename(const String & new_path_to_table_data, const String & new_database_name, const String & new_table_name,
                                TableStructureWriteLockHolder &)
{
    if (!relative_data_path.empty())
        renameOnDisk(new_path_to_table_data);
    renameInMemory(new_database_name, new_table_name);
}
void StorageDistributed::renameOnDisk(const String & new_path_to_table_data)
{
    for (const DiskPtr & disk : volume->disks)
    {
        const String path(disk->getPath());
        auto new_path = path + new_path_to_table_data;
        Poco::File(path + relative_data_path).renameTo(new_path);

        LOG_DEBUG(log, "Updating path to " << new_path);

        std::lock_guard lock(cluster_nodes_mutex);
        for (auto & node : cluster_nodes_data)
            node.second.directory_monitor->updatePath(new_path);
    }

    relative_data_path = new_path_to_table_data;
}


void registerStorageDistributed(StorageFactory & factory)
{
    factory.registerStorage("Distributed", [](const StorageFactory::Arguments & args)
    {
        /** Arguments of engine is following:
          * - name of cluster in configuration;
          * - name of remote database;
          * - name of remote table;
          * - policy to store data in;
          *
          * Remote database may be specified in following form:
          * - identifier;
          * - constant expression with string result, like currentDatabase();
          * -- string literal as specific case;
          * - empty string means 'use default database from cluster'.
          */

        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 3 || engine_args.size() > 5)
            throw Exception(
                "Storage Distributed requires from 3 to 5 parameters - "
                "name of configuration section with list of remote servers, "
                "name of remote database, "
                "name of remote table, "
                "sharding key expression (optional), "
                "policy to store data in (optional).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String cluster_name = getClusterNameAndMakeLiteral(engine_args[0]);

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);
        engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);

        String remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        String remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        const auto & sharding_key = engine_args.size() >= 4 ? engine_args[3] : nullptr;
        const auto & storage_policy = engine_args.size() >= 5 ? engine_args[4]->as<ASTLiteral &>().value.safeGet<String>() : "";

        /// Check that sharding_key exists in the table and has numeric type.
        if (sharding_key)
        {
            auto sharding_expr = buildShardingKeyExpression(sharding_key, args.context, args.columns.getAllPhysical(), true);
            const Block & block = sharding_expr->getSampleBlock();

            if (block.columns() != 1)
                throw Exception("Sharding expression must return exactly one column", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

            auto type = block.getByPosition(0).type;

            if (!type->isValueRepresentedByInteger())
                throw Exception("Sharding expression has type " + type->getName() +
                    ", but should be one of integer type", ErrorCodes::TYPE_MISMATCH);
        }

        return StorageDistributed::create(
            args.table_id, args.columns, args.constraints,
            remote_database, remote_table, cluster_name,
            args.context,
            sharding_key,
            storage_policy,
            args.relative_data_path,
            args.attach);
    });
}

}
