#include <Storages/StorageFilesystemCache.h>

#include <Access/Common/AccessFlags.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageSnapshot.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Walks the priority queue on the first call to `generate` and emits the result in chunks of at most `max_block_size` rows.
class FilesystemCacheSource final : public ISource
{
public:
    FilesystemCacheSource(
        SharedHeader header_,
        FileCachePtr cache_,
        UInt64 query_max_elements_,
        UInt64 query_max_total_size_,
        IFileCachePriority::IterationOrder query_order_,
        size_t max_block_size_)
        : ISource(std::move(header_))
        , cache(std::move(cache_))
        , query_max_elements(query_max_elements_)
        , query_max_total_size(query_max_total_size_)
        , query_order(query_order_)
        , max_block_size(std::max<size_t>(max_block_size_, 1))
    {
    }

    String getName() const override { return "FilesystemCache"; }

protected:
    Chunk generate() override
    {
        if (!infos)
            infos = cache->observeFilesystemCache(query_max_elements, query_max_total_size, query_order, &getCancellationFlag());

        if (next_row == infos->size())
            return {};

        const size_t begin = next_row;
        const size_t end = begin + std::min(max_block_size, infos->size() - begin);
        next_row = end;

        const auto & header = getPort().getHeader();
        MutableColumns columns = header.cloneEmptyColumns();
        for (size_t i = 0; i < header.columns(); ++i)
        {
            auto & column = *columns[i];
            column.reserve(end - begin);
            auto fill = [&](auto && get_value)
            {
                for (size_t row = begin; row < end; ++row)
                    column.insert(get_value((*infos)[row]));
            };

            const auto & name = header.getByPosition(i).name;
            if (name == "priority_rank")
                fill([](const FileSegmentSummaryInfo & info) { return info.priority_rank; });
            else if (name == "path")
                fill([](const FileSegmentSummaryInfo & info) { return info.path; });
            else if (name == "key")
                fill([](const FileSegmentSummaryInfo & info) { return info.key; });
            else if (name == "offset")
                fill([](const FileSegmentSummaryInfo & info) { return info.offset; });
            else if (name == "size")
                fill([](const FileSegmentSummaryInfo & info) { return info.size; });
            else if (name == "state")
                fill([](const FileSegmentSummaryInfo & info) { return info.state; });
            else if (name == "queue")
                fill([](const FileSegmentSummaryInfo & info) { return info.queue_entry_type; });
            else if (name == "user_id")
                fill([](const FileSegmentSummaryInfo & info) { return info.user_id; });
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} in table function `filesystemCache`", name);
        }

        return Chunk(std::move(columns), end - begin);
    }

private:
    const FileCachePtr cache;
    const UInt64 query_max_elements;
    const UInt64 query_max_total_size;
    const IFileCachePriority::IterationOrder query_order;
    const size_t max_block_size;

    std::optional<FileSegmentSummaryInfoList> infos;
    size_t next_row = 0;
};

}

StorageFilesystemCache::StorageFilesystemCache(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    String cache_name_,
    UInt64 query_max_elements_,
    UInt64 query_max_total_size_,
    IFileCachePriority::IterationOrder query_order_)
    : IStorage(table_id_)
    , cache_name(std::move(cache_name_))
    , query_max_elements(query_max_elements_)
    , query_max_total_size(query_max_total_size_)
    , query_order(query_order_)
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns_);
    setInMemoryMetadata(metadata);
}

ColumnsDescription StorageFilesystemCache::getColumnsDescription()
{
    return ColumnsDescription{
        {"priority_rank", std::make_shared<DataTypeUInt64>(),
         "1-based position of the segment. For 'hottest' and 'coldest': in the output. For 'random': in the queues of the cache "
         "walked coldest first (SLRU: probationary, then protected; split cache: data, then system), "
         "not counting the segments which were skipped because they were removed from the cache or in use during the query."},
        {"path", std::make_shared<DataTypeString>(), "The file of the segment on disk, including the `_<size>` suffix of downloaded segments."},
        {"key", std::make_shared<DataTypeString>(), "Cache key."},
        {"offset", std::make_shared<DataTypeUInt64>(), "Offset of the segment in the source file."},
        {"size", std::make_shared<DataTypeUInt64>(), "Size of the segment as accounted by the cache."},
        {"state", std::make_shared<DataTypeString>(), "Download state of the segment, e.g. `DOWNLOADED`."},
        {"queue", std::make_shared<DataTypeString>(),
         "The queue of the segment: `LRU`, `SLRU_Protected`, `SLRU_Probationary`, `SplitCache_Data` or `SplitCache_System`."},
        {"user_id", std::make_shared<DataTypeString>(), "Owner of the key."},
    };
}

Pipe StorageFilesystemCache::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /* query_info */,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t /* num_streams */)
{
    /// It shows the same segments as `system.filesystem_cache`, so it requires the same privilege.
    context->checkAccess(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "filesystem_cache");

    storage_snapshot->check(column_names);

    auto cache = FileCacheFactory::instance().get(cache_name);
    auto header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
    return Pipe(std::make_shared<FilesystemCacheSource>(
        std::move(header), std::move(cache), query_max_elements, query_max_total_size, query_order, max_block_size));
}

}
