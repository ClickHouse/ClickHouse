#include <Storages/System/StorageSystemObjectStorageQueueMetadata.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/StreamingStorageRegistry.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
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
        {"processed_nodes", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Number of nodes in the `processed` folder in keeper. Only set for `unordered` mode: in `ordered` mode there are no per-file processed nodes (see `processed_path` instead), so the value is NULL."},
        {"processing_nodes", std::make_shared<DataTypeUInt64>(), "Number of nodes in the `processing` folder in keeper"},
        {"failed_nodes", std::make_shared<DataTypeUInt64>(), "Number of nodes in the `failed` folder in keeper"},
        {"processed", map_string_string,
            "Contents (node name -> node data) of the `processed` folder in keeper. Only filled for `unordered` mode. Fetched only when this column is selected."},
        {"processing", map_string_string, "Contents (node name -> node data) of the `processing` folder in keeper. Fetched only when this column is selected."},
        {"failed", map_string_string, "Contents (node name -> node data) of the `failed` folder in keeper. Fetched only when this column is selected."},
        {"processed_path", map_string_string,
            "Last processed path per processed pointer in keeper (relative pointer path -> last processed file path). Only filled for `ordered` mode, where it covers the single, per-bucket and per-partition pointers. Fetched only when this column is selected."},
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
    /// Source column indexes.
    enum Column : size_t
    {
        ZOOKEEPER_PATH = 0,
        PROCESSED_NODES,
        PROCESSING_NODES,
        FAILED_NODES,
        PROCESSED,
        PROCESSING,
        FAILED,
        PROCESSED_PATH,
    };

    auto log = getLogger(name);
    /// All Keeper requests below must run with a component set, otherwise
    /// `Coordination::ZooKeeper::pushRequest` throws a logical error.
    auto component_guard = Coordination::setCurrentComponent("StorageSystemObjectStorageQueueMetadata::fillData");

    for (const auto & [zookeeper_path, metadata] : ObjectStorageQueueMetadataFactory::instance().getAll())
    {
        if (type != metadata->getType())
            continue;

        /// `zookeeper_path` is the factory key, which for auxiliary Keepers is
        /// prefixed with "<keeper>:". It is used only for display. Keeper reads
        /// must use the raw path returned by `getPath`; the client returned by
        /// `getZooKeeper` is already bound to the right Keeper.
        const std::filesystem::path base_path(metadata->getPath());
        const bool unordered = metadata->getTableMetadata().mode == "unordered";
        const size_t batch_size = std::max<size_t>(1, metadata->getKeeperMultireadBatchSize());

        auto zk_retries = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);

        /// Read the data of `paths` in batches, to avoid building a single huge
        /// multiread request that could exceed keeper limits.
        auto batched_get = [&](const std::vector<std::string> & paths)
        {
            std::vector<std::optional<std::string>> data(paths.size());
            for (size_t offset = 0; offset < paths.size(); offset += batch_size)
            {
                const std::vector<std::string> batch(
                    paths.begin() + offset,
                    paths.begin() + std::min(offset + batch_size, paths.size()));

                zkutil::ZooKeeper::MultiTryGetResponse responses;
                zk_retries.resetFailures();
                zk_retries.retryLoop([&] { responses = metadata->getZooKeeper()->tryGet(batch); });

                for (size_t i = 0; i < batch.size(); ++i)
                {
                    /// A node may have been removed between listing and reading it.
                    if (responses[i].error == Coordination::Error::ZOK)
                        data[offset + i] = responses[i].data;
                }
            }
            return data;
        };

        /// Read a `processed`/`processing`/`failed` folder of per-file nodes.
        /// When only the count is needed, reading the stat is enough; the
        /// children are listed and fetched only when the contents are requested.
        auto read_folder = [&](const std::string & folder, bool need_count, bool need_contents,
                               UInt64 & count_out, std::vector<std::pair<String, String>> & contents_out)
        {
            if (!need_count && !need_contents)
                return;

            const std::string folder_path = base_path / folder;
            if (!need_contents)
            {
                Coordination::Stat stat;
                bool exists = false;
                zk_retries.resetFailures();
                zk_retries.retryLoop([&] { exists = metadata->getZooKeeper()->exists(folder_path, &stat); });
                count_out = exists ? stat.numChildren : 0;
                return;
            }

            Strings nodes;
            Coordination::Error code = Coordination::Error::ZOK;
            zk_retries.resetFailures();
            zk_retries.retryLoop([&] { code = metadata->getZooKeeper()->tryGetChildren(folder_path, nodes); });
            if (code == Coordination::Error::ZNONODE)
                return;
            if (code != Coordination::Error::ZOK)
                throw zkutil::KeeperException::fromPath(code, folder_path);

            count_out = nodes.size();

            std::vector<std::string> node_paths;
            node_paths.reserve(nodes.size());
            for (const auto & node : nodes)
                node_paths.push_back(base_path / folder / node);

            const auto data = batched_get(node_paths);

            contents_out.reserve(nodes.size());
            for (size_t i = 0; i < nodes.size(); ++i)
            {
                if (data[i].has_value())
                    contents_out.emplace_back(nodes[i], *data[i]);
            }
        };

        /// In ordered mode the `processed` folder is not a set of per-file nodes
        /// but a compact "last processed" pointer. It can be a single node, one
        /// per bucket (buckets > 1), or one per partition (HIVE/REGEX) - and any
        /// combination of the two. Collect them all as (relative path -> last
        /// processed file path).
        auto read_processed_pointers = [&]() -> std::vector<std::pair<String, String>>
        {
            std::vector<std::string> roots;
            if (metadata->useBucketsForProcessing())
            {
                for (size_t bucket = 0; bucket < metadata->getBucketsNum(); ++bucket)
                    roots.push_back(base_path / "buckets" / toString(bucket) / "processed");
            }
            else
            {
                roots.push_back(base_path / "processed");
            }

            std::vector<std::string> leaf_paths;
            const bool partitioned = metadata->getPartitioningMode() != ObjectStorageQueuePartitioningMode::NONE;
            if (!partitioned)
            {
                leaf_paths = std::move(roots);
            }
            else
            {
                /// Each root is a directory whose children are the partitions.
                for (const auto & root : roots)
                {
                    Strings partitions;
                    Coordination::Error code = Coordination::Error::ZOK;
                    zk_retries.resetFailures();
                    zk_retries.retryLoop([&] { code = metadata->getZooKeeper()->tryGetChildren(root, partitions); });
                    if (code == Coordination::Error::ZNONODE)
                        continue;
                    if (code != Coordination::Error::ZOK)
                        throw zkutil::KeeperException::fromPath(code, root);
                    for (const auto & partition : partitions)
                        leaf_paths.push_back(std::filesystem::path(root) / partition);
                }
            }

            if (leaf_paths.empty())
                return {};

            const auto data = batched_get(leaf_paths);

            std::vector<std::pair<String, String>> result;
            result.reserve(leaf_paths.size());
            for (size_t i = 0; i < leaf_paths.size(); ++i)
            {
                if (!data[i].has_value() || data[i]->empty())
                    continue;
                /// Partition pointers store the raw last processed file path
                /// directly; only the root/bucket pointers are `NodeMetadata` JSON.
                std::string file_path = partitioned
                    ? *data[i]
                    : ObjectStorageQueueIFileMetadata::NodeMetadata::fromString(*data[i]).file_path;
                auto key = std::filesystem::path(leaf_paths[i]).lexically_relative(base_path).string();
                result.emplace_back(std::move(key), std::move(file_path));
            }
            return result;
        };

        UInt64 processed_count = 0;
        UInt64 processing_count = 0;
        UInt64 failed_count = 0;
        std::vector<std::pair<String, String>> processed_contents;
        std::vector<std::pair<String, String>> processing_contents;
        std::vector<std::pair<String, String>> failed_contents;
        std::vector<std::pair<String, String>> processed_path;

        read_folder("processing", columns_mask[PROCESSING_NODES], columns_mask[PROCESSING], processing_count, processing_contents);
        read_folder("failed", columns_mask[FAILED_NODES], columns_mask[FAILED], failed_count, failed_contents);
        if (unordered)
            read_folder("processed", columns_mask[PROCESSED_NODES], columns_mask[PROCESSED], processed_count, processed_contents);
        else if (columns_mask[PROCESSED_PATH])
            processed_path = read_processed_pointers();

        size_t src_index = 0;
        size_t res_index = 0;

        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(zookeeper_path);

        /// processed_nodes is only meaningful in unordered mode.
        if (columns_mask[src_index++])
        {
            if (unordered)
                res_columns[res_index++]->insert(processed_count);
            else
                res_columns[res_index++]->insertDefault();
        }
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(processing_count);
        if (columns_mask[src_index++])
            res_columns[res_index++]->insert(failed_count);

        if (columns_mask[src_index++])
            insertMap(*res_columns[res_index++], processed_contents);
        if (columns_mask[src_index++])
            insertMap(*res_columns[res_index++], processing_contents);
        if (columns_mask[src_index++])
            insertMap(*res_columns[res_index++], failed_contents);

        if (columns_mask[src_index++])
            insertMap(*res_columns[res_index++], processed_path);
    }
}

template class StorageSystemObjectStorageQueueMetadata<ObjectStorageType::S3>;
template class StorageSystemObjectStorageQueueMetadata<ObjectStorageType::Azure>;

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemObjectStorageQueueMetadata<ObjectStorageType::Azure>) }
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemObjectStorageQueueMetadata<ObjectStorageType::S3>) }
