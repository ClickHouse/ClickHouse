#include <Storages/System/StorageSystemKeeperStorage.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#if USE_NURAFT

#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperNodesStorage.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>

namespace DB
{

ColumnsDescription StorageSystemKeeperStorage::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"path", std::make_shared<DataTypeString>(), "Absolute path of the node."},
        {"data", std::make_shared<DataTypeString>(), "Data stored in the node."},
        {"czxid", std::make_shared<DataTypeInt64>(), "ID of the transaction that created the node."},
        {"mzxid", std::make_shared<DataTypeInt64>(), "ID of the transaction that last modified the node."},
        {"ctime", std::make_shared<DataTypeDateTime64>(3), "Time when the node was created."},
        {"mtime", std::make_shared<DataTypeDateTime64>(3), "Time when the node was last modified."},
        {"version", std::make_shared<DataTypeInt32>(), "Number of modifications of the node data."},
        {"cversion", std::make_shared<DataTypeInt32>(), "Number of modifications of the node children."},
        {"aversion", std::make_shared<DataTypeInt32>(), "Number of modifications of the node ACL."},
        {"ephemeral_owner", std::make_shared<DataTypeInt64>(), "ID of the session that owns the node if it is ephemeral, 0 otherwise."},
        {"data_length", std::make_shared<DataTypeUInt32>(), "Size of the node data in bytes."},
        {"num_children", std::make_shared<DataTypeInt32>(), "Number of children of the node."},
        {"pzxid", std::make_shared<DataTypeInt64>(), "ID of the transaction that last added or removed children of the node."},
        {"seq_num", std::make_shared<DataTypeInt64>(), "Counter used to generate names of sequential children of the node."},
        {"ttl", std::make_shared<DataTypeInt64>(), "TTL of the node in milliseconds if it is a TTL node, 0 otherwise."},
        {"acl_id", std::make_shared<DataTypeUInt32>(), "Internal ID of the node ACL, 0 if the node has no ACL."},
    };
}

namespace
{

class SystemKeeperStorageSource final : public ISource
{
public:
    SystemKeeperStorageSource(std::unique_ptr<KeeperNodesReadView> view_, Block header, std::vector<UInt8> columns_mask_, UInt64 max_block_size_)
        : ISource(std::make_shared<const Block>(std::move(header)))
        , view(std::move(view_))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
    {
        addTotalRowsApprox(view->getNodeCount());
    }

    String getName() const override { return "SystemKeeperStorage"; }

protected:
    Chunk generate() override
    {
        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        size_t rows_count = 0;
        std::string_view path;
        std::string_view data;
        KeeperNodeStats stats;
        while (rows_count < max_block_size && view->next(path, data, stats))
        {
            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insertData(path.data(), path.size());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insertData(data.data(), data.size());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.czxid);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.mzxid);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(DecimalField<DateTime64>(stats.getCTime(), 3));
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(DecimalField<DateTime64>(stats.mtime, 3));
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.version);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.cversion);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.aversion);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.getEphemeralOwner());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.data_size);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.getNumChildren());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.pzxid);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.getSeqNum());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.getTTL());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(stats.acl_id);

            ++rows_count;
        }

        if (rows_count == 0)
            return {};

        return Chunk(std::move(res_columns), rows_count);
    }

    void onFinish() override { view.reset(); }

private:
    std::unique_ptr<KeeperNodesReadView> view;
    const std::vector<UInt8> columns_mask;
    const UInt64 max_block_size;
};

}

StorageSystemKeeperStorage::StorageSystemKeeperStorage(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription());
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemKeeperStorage::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

Pipe StorageSystemKeeperStorage::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    auto dispatcher = context->tryGetKeeperDispatcher();
    if (!dispatcher)
        return {};

    auto [columns_mask, res_block] = getQueriedColumnsMaskAndHeader(storage_snapshot->metadata->getSampleBlock(), column_names);

    return Pipe(std::make_shared<SystemKeeperStorageSource>(
        dispatcher->getStateMachine().getStorageReadView(), std::move(res_block), std::move(columns_mask), max_block_size));
}

}


/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemKeeperStorage) }

#endif
