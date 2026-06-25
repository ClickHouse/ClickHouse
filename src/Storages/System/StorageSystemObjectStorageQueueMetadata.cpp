#include <Storages/System/StorageSystemObjectStorageQueueMetadata.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/StreamingStorageRegistry.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>


namespace DB
{

template <ObjectStorageType type>
ColumnsDescription StorageSystemObjectStorageQueueMetadata<type>::getColumnsDescription()
{
    auto map_string_string = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    return ColumnsDescription
    {
        {"zookeeper_path", std::make_shared<DataTypeString>(), "Path in zookeeper to metadata"},
        {"processed_nodes", std::make_shared<DataTypeUInt64>(), "Number of nodes in the `processed` folder in keeper"},
        {"processing_nodes", std::make_shared<DataTypeUInt64>(), "Number of nodes in the `processing` folder in keeper"},
        {"failed_nodes", std::make_shared<DataTypeUInt64>(), "Number of nodes in the `failed` folder in keeper"},
        {"processed", map_string_string, "Contents (node name -> node data) of the `processed` folder in keeper. Fetched only when this column is selected."},
        {"processing", map_string_string, "Contents (node name -> node data) of the `processing` folder in keeper. Fetched only when this column is selected."},
        {"failed", map_string_string, "Contents (node name -> node data) of the `failed` folder in keeper. Fetched only when this column is selected."},
    };
}

template <ObjectStorageType type>
StorageSystemObjectStorageQueueMetadata<type>::StorageSystemObjectStorageQueueMetadata(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

namespace
{

void insertMap(IColumn & column, const std::vector<std::pair<String, String>> & contents)
{
    auto & column_map = assert_cast<ColumnMap &>(column);
    auto & offsets = column_map.getNestedColumn().getOffsets();
    auto & tuple_column = column_map.getNestedData();
    auto & key_column = tuple_column.getColumn(0);
    auto & value_column = tuple_column.getColumn(1);

    for (const auto & [name, data] : contents)
    {
        key_column.insert(name);
        value_column.insert(data);
    }
    offsets.push_back(offsets.back() + contents.size());
}

}

template <ObjectStorageType type>
void StorageSystemObjectStorageQueueMetadata<type>::fillData(
    MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const
{
    /// Source columns are ordered as: zookeeper_path, then per folder
    /// {processed, processing, failed} a `<folder>_nodes` count column,
    /// and finally the per folder `<folder>` contents columns.
    static constexpr std::array folders{"processed", "processing", "failed"};
    static constexpr size_t count_column_offset = 1;
    static constexpr size_t contents_column_offset = count_column_offset + folders.size();

    auto log = getLogger(name);
    for (const auto & [zookeeper_path, metadata] : ObjectStorageQueueMetadataFactory::instance().getAll())
    {
        if (type != metadata->getType())
            continue;

        const std::filesystem::path base_path(zookeeper_path);

        /// For each folder compute its number of nodes (and, when requested,
        /// the contents) - but only touch keeper for the folders actually queried.
        std::array<UInt64, folders.size()> counts{};
        std::array<std::vector<std::pair<String, String>>, folders.size()> contents;

        auto zk_retries = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
        for (size_t f = 0; f < folders.size(); ++f)
        {
            const bool need_count = columns_mask[count_column_offset + f];
            const bool need_contents = columns_mask[contents_column_offset + f];
            if (!need_count && !need_contents)
                continue;

            const std::string folder_path = base_path / folders[f];

            if (!need_contents)
            {
                /// Only the count is requested - reading the stat is enough.
                Coordination::Stat stat;
                bool exists = false;
                zk_retries.resetFailures();
                zk_retries.retryLoop([&] { exists = metadata->getZooKeeper()->exists(folder_path, &stat); });
                counts[f] = exists ? stat.numChildren : 0;
                continue;
            }

            Strings nodes;
            Coordination::Error code = Coordination::Error::ZOK;
            zk_retries.resetFailures();
            zk_retries.retryLoop([&] { code = metadata->getZooKeeper()->tryGetChildren(folder_path, nodes); });
            if (code == Coordination::Error::ZNONODE)
                continue;
            if (code != Coordination::Error::ZOK)
                throw zkutil::KeeperException::fromPath(code, folder_path);

            counts[f] = nodes.size();

            std::vector<std::string> node_paths;
            node_paths.reserve(nodes.size());
            for (const auto & node : nodes)
                node_paths.push_back(base_path / folders[f] / node);

            zkutil::ZooKeeper::MultiTryGetResponse responses;
            zk_retries.resetFailures();
            zk_retries.retryLoop([&] { responses = metadata->getZooKeeper()->tryGet(node_paths); });

            contents[f].reserve(nodes.size());
            for (size_t i = 0; i < nodes.size(); ++i)
            {
                /// A node may have been removed between listing and reading it.
                if (responses[i].error == Coordination::Error::ZOK)
                    contents[f].emplace_back(nodes[i], responses[i].data);
            }
        }

        size_t src_index = 0;
        size_t res_index = 0;

        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(zookeeper_path);

        for (size_t f = 0; f < folders.size(); ++f)
        {
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(counts[f]);
        }

        for (size_t f = 0; f < folders.size(); ++f)
        {
            if (columns_mask[src_index++])
                insertMap(*res_columns[res_index++], contents[f]);
        }
    }
}

template class StorageSystemObjectStorageQueueMetadata<ObjectStorageType::S3>;
template class StorageSystemObjectStorageQueueMetadata<ObjectStorageType::Azure>;

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemObjectStorageQueueMetadata<ObjectStorageType::Azure>) }
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemObjectStorageQueueMetadata<ObjectStorageType::S3>) }
