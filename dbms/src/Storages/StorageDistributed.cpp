#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

#include <Databases/IDatabase.h>

#include <DataTypes/DataTypeFactory.h>

#include <Storages/StorageDistributed.h>
#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageFactory.h>

#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTDropQuery.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/getClusterName.h>

#include <Core/Field.h>

#include <IO/ReadHelpers.h>

#include <Poco/DirectoryIterator.h>

#include <memory>
#include <boost/filesystem.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int STORAGE_REQUIRES_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int READONLY;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INFINITE_LOOP;
    extern const int TYPE_MISMATCH;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


namespace
{

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
ASTPtr rewriteSelectQuery(const ASTPtr & query, const std::string & database, const std::string & table, ASTPtr table_function_ptr = nullptr)
{
    auto modified_query_ast = query->clone();
    if (table_function_ptr)
        typeid_cast<ASTSelectQuery &>(*modified_query_ast).addTableFunction(table_function_ptr);
    else
        typeid_cast<ASTSelectQuery &>(*modified_query_ast).replaceDatabaseAndTable(database, table);
    return modified_query_ast;
}

/// insert query has database and table names as bare strings
/// If the query is null, it creates a insert query with the database and tables
/// Or it creates a copy of query, changes the database and table names.
ASTPtr rewriteInsertQuery(const ASTPtr & query, const std::string & database, const std::string & table)
{
    ASTPtr modified_query_ast = nullptr;
    if (query == nullptr)
        modified_query_ast = std::make_shared<ASTInsertQuery>();
    else
        modified_query_ast = query->clone();

    auto & actual_query = typeid_cast<ASTInsertQuery &>(*modified_query_ast);
    actual_query.database = database;
    actual_query.table = table;
    actual_query.table_function = nullptr;
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
    const String & database_name,
    const String & table_name_,
    const ColumnsDescription & columns_,
    const String & remote_database_,
    const String & remote_table_,
    const String & cluster_name_,
    const Context & context_,
    const ASTPtr & sharding_key_,
    const String & data_path_,
    bool attach)
    : IStorage{columns_},
    table_name(table_name_),
    remote_database(remote_database_), remote_table(remote_table_),
    context(context_), cluster_name(context.getMacros()->expand(cluster_name_)), has_sharding_key(sharding_key_),
      sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, nullptr, getColumns().getAllPhysical()).getActions(false) : nullptr),
    sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
    path(data_path_.empty() ? "" : (data_path_ + escapeForFileName(table_name) + '/'))
{
    /// Sanity check. Skip check if the table is already created to allow the server to start.
    if (!attach && !cluster_name.empty())
    {
        size_t num_local_shards = context.getCluster(cluster_name)->getLocalShardCount();
        if (num_local_shards && remote_database == database_name && remote_table == table_name)
            throw Exception("Distributed table " + table_name + " looks at itself", ErrorCodes::INFINITE_LOOP);
    }
}


StorageDistributed::StorageDistributed(
    const String & database_name,
    const String & table_name_,
    const ColumnsDescription & columns_,
    ASTPtr remote_table_function_ptr_,
    const String & cluster_name_,
    const Context & context_,
    const ASTPtr & sharding_key_,
    const String & data_path_,
    bool attach)
    : StorageDistributed(database_name, table_name_, columns_, String{}, String{}, cluster_name_, context_, sharding_key_, data_path_, attach)
{
        remote_table_function_ptr = remote_table_function_ptr_;
}


StoragePtr StorageDistributed::createWithOwnCluster(
    const std::string & table_name_,
    const ColumnsDescription & columns_,
    const String & remote_database_,       /// database on remote servers.
    const String & remote_table_,          /// The name of the table on the remote servers.
    ClusterPtr owned_cluster_,
    const Context & context_)
{
    auto res = ext::shared_ptr_helper<StorageDistributed>::create(
        String{}, table_name_, columns_, remote_database_, remote_table_, String{}, context_, ASTPtr(), String(), false);

    res->owned_cluster = owned_cluster_;

    return res;
}


StoragePtr StorageDistributed::createWithOwnCluster(
    const std::string & table_name_,
    const ColumnsDescription & columns_,
    ASTPtr & remote_table_function_ptr_,
    ClusterPtr & owned_cluster_,
    const Context & context_)
{
    auto res = ext::shared_ptr_helper<StorageDistributed>::create(
        String{}, table_name_, columns_, remote_table_function_ptr_, String{}, context_, ASTPtr(), String(), false);

    res->owned_cluster = owned_cluster_;

    return res;
}

QueryProcessingStage::Enum StorageDistributed::getQueryProcessingStage(const Context & context) const
{
    auto cluster = getCluster();
    return getQueryProcessingStage(context, cluster);
}

QueryProcessingStage::Enum StorageDistributed::getQueryProcessingStage(const Context & context, const ClusterPtr & cluster) const
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

BlockInputStreams StorageDistributed::read(
    const Names & /*column_names*/,
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

    Block header = materializeBlock(InterpreterSelectQuery(query_info.query, context, Names{}, processed_stage).getSampleBlock());

    ClusterProxy::SelectStreamFactory select_stream_factory = remote_table_function_ptr
        ? ClusterProxy::SelectStreamFactory(
            header, processed_stage, remote_table_function_ptr, context.getExternalTables())
        : ClusterProxy::SelectStreamFactory(
            header, processed_stage, QualifiedTableName{remote_database, remote_table}, context.getExternalTables());

    return ClusterProxy::executeQuery(
        select_stream_factory, cluster, modified_query_ast, context, settings);
}


BlockOutputStreamPtr StorageDistributed::write(const ASTPtr & query, const Settings & settings)
{
    auto cluster = getCluster();

    /// Ban an attempt to make async insert into the table belonging to DatabaseMemory
    if (path.empty() && !owned_cluster && !settings.insert_distributed_sync.value)
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
        *this, rewriteInsertQuery(query, remote_database, remote_table), cluster, settings, insert_sync, timeout);
}


void StorageDistributed::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
    for (const auto & param : params)
        if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
            throw Exception("Storage engine " + getName() + " doesn't support primary key.", ErrorCodes::NOT_IMPLEMENTED);

    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    ColumnsDescription new_columns = getColumns();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, {});
    setColumns(std::move(new_columns));
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


void StorageDistributed::truncate(const ASTPtr &)
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
    std::lock_guard lock(cluster_nodes_mutex);
    cluster_nodes_data[name].requireDirectoryMonitor(name, *this);
}

ConnectionPoolPtr StorageDistributed::requireConnectionPool(const std::string & name)
{
    std::lock_guard lock(cluster_nodes_mutex);
    auto & node_data = cluster_nodes_data[name];
    node_data.requireConnectionPool(name, *this);
    return node_data.conneciton_pool;
}

size_t StorageDistributed::getShardCount() const
{
    return getCluster()->getShardCount();
}

ClusterPtr StorageDistributed::getCluster() const
{
    return owned_cluster ? owned_cluster : context.getCluster(cluster_name);
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

void StorageDistributed::ClusterNodeData::shutdownAndDropAllData()
{
    directory_monitor->shutdownAndDropAllData();
}


void registerStorageDistributed(StorageFactory & factory)
{
    factory.registerStorage("Distributed", [](const StorageFactory::Arguments & args)
    {
        /** Arguments of engine is following:
          * - name of cluster in configuration;
          * - name of remote database;
          * - name of remote table;
          *
          * Remote database may be specified in following form:
          * - identifier;
          * - constant expression with string result, like currentDatabase();
          * -- string literal as specific case;
          * - empty string means 'use default database from cluster'.
          */

        ASTs & engine_args = args.engine_args;

        if (!(engine_args.size() == 3 || engine_args.size() == 4))
            throw Exception("Storage Distributed requires 3 or 4 parameters"
                " - name of configuration section with list of remote servers, name of remote database, name of remote table,"
                " sharding key expression (optional).", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String cluster_name = getClusterName(*engine_args[0]);

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);
        engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);

        String remote_database = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();
        String remote_table = static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>();

        const auto & sharding_key = engine_args.size() == 4 ? engine_args[3] : nullptr;

        /// Check that sharding_key exists in the table and has numeric type.
        if (sharding_key)
        {
            auto sharding_expr = ExpressionAnalyzer(sharding_key, args.context, nullptr, args.columns.getAllPhysical()).getActions(true);
            const Block & block = sharding_expr->getSampleBlock();

            if (block.columns() != 1)
                throw Exception("Sharding expression must return exactly one column", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

            auto type = block.getByPosition(0).type;

            if (!type->isValueRepresentedByInteger())
                throw Exception("Sharding expression has type " + type->getName() +
                    ", but should be one of integer type", ErrorCodes::TYPE_MISMATCH);
        }

        return StorageDistributed::create(
            args.database_name, args.table_name, args.columns,
            remote_database, remote_table, cluster_name,
            args.context, sharding_key, args.data_path,
            args.attach);
    });
}

}
