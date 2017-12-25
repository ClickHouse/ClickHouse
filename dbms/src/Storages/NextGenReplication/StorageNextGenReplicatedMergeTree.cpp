#include <Storages/NextGenReplication/StorageNextGenReplicatedMergeTree.h>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int INCORRECT_DATA;
}


StorageNextGenReplicatedMergeTree::StorageNextGenReplicatedMergeTree(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const String & path_, const String & database_name_, const String & name_,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    Context & context_,
    const ASTPtr & primary_expr_ast_,
    const String & date_column_name,
    const ASTPtr & partition_expr_ast_,
    const ASTPtr & sampling_expression_,
    const MergeTreeData::MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool has_force_restore_data_flag)
    : IStorage(columns_, materialized_columns_, alias_columns_, column_defaults_)
    , context(context_)
    , log(&Logger::get(database_name_ + "." + name_ + " (StorageNextGenReplicatedMergeTree)"))
    , database_name(database_name_), table_name(name_), full_path(path_ + escapeForFileName(table_name) + '/')

    , data(
        database_name, table_name, full_path,
        columns_, materialized_columns_, alias_columns_, column_defaults_,
        context,
        primary_expr_ast_, date_column_name, partition_expr_ast_, sampling_expression_,
        merging_params_, settings_, true, attach,
        [this] (const String & /* part_name */) { /* TODO: enqueue part for check */; })
    , reader(data), writer(data), merger(data, context.getBackgroundPool())
    , zookeeper_path(context.getMacros().expand(zookeeper_path_))
    , replica_name(context.getMacros().expand(replica_name_))
    , fetcher(data)
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should starts with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    replica_path = zookeeper_path + "/replicas/" + replica_name;

    bool skip_sanity_checks = has_force_restore_data_flag; /// TODO: load the flag from ZK.

    data.loadDataParts(skip_sanity_checks);

    auto current_zookeeper = context.getZooKeeper();

    if (!current_zookeeper)
    {
        if (!attach)
            throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        LOG_ERROR(log, "No ZooKeeper: the table will be in readonly mode.");
        is_readonly = true;
        return;
    }

    if (!attach)
    {
        if (!data.getDataParts().empty())
            throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

        createTableOrReplica();
    }
    else
    {
        /// Temporary directories contain unfinalized results of Merges or Fetches (after forced restart)
        ///  and don't allow to reinitialize them, so delete each of them immediately.
        data.clearOldTemporaryDirectories(0);
    }

    /// TODO: Check table structure.
}

StorageNextGenReplicatedMergeTree::~StorageNextGenReplicatedMergeTree()
{
    try
    {
        shutdown();
    }
    catch(...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageNextGenReplicatedMergeTree::startup()
{
    if (is_readonly)
        return;

    StoragePtr ptr = shared_from_this();
    InterserverIOEndpointPtr data_parts_exchange_endpoint = std::make_shared<DataPartsExchange::Service>(data, ptr);
    data_parts_exchange_endpoint_holder = std::make_shared<InterserverIOEndpointHolder>(
        data_parts_exchange_endpoint->getId(replica_path), data_parts_exchange_endpoint, context.getInterserverIOHandler());
}

void StorageNextGenReplicatedMergeTree::createTableOrReplica()
{
    /// TODO
}

void StorageNextGenReplicatedMergeTree::shutdown()
{
    /** This must be done before waiting for restarting_thread.
      * Because restarting_thread will wait for finishing of tasks in background pool,
      *  and parts are fetched in that tasks.
      */
    fetcher.blocker.cancelForever();

    if (data_parts_exchange_endpoint_holder)
    {
        data_parts_exchange_endpoint_holder->cancelForever();
        data_parts_exchange_endpoint_holder = nullptr;
    }
}

BlockInputStreams StorageNextGenReplicatedMergeTree::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    /// TODO: quorum reads.

    return reader.read(
        column_names, query_info, context, processed_stage, max_block_size, num_streams, 0);
}

BlockOutputStreamPtr StorageNextGenReplicatedMergeTree::write(const ASTPtr & /*query*/, const Settings &)
{
    /// TODO
    return {};
}

void StorageNextGenReplicatedMergeTree::drop()
{
    data.dropAllData();
    /// TODO: remove from ZK
}

zkutil::ZooKeeperPtr StorageNextGenReplicatedMergeTree::tryGetZooKeeper()
{
    /// TODO: async reconnect?
    return context.tryGetZooKeeper();
}

zkutil::ZooKeeperPtr StorageNextGenReplicatedMergeTree::getZooKeeper()
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}

}
