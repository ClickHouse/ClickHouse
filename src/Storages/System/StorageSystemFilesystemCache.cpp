#include <Storages/System/StorageSystemFilesystemCache.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Disks/IDisk.h>


namespace DB
{
namespace
{
class SystemFilesystemCacheSource : public ISource, private WithContext
{
public:
    SystemFilesystemCacheSource(
        SharedHeader header_,
        UInt64 max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , max_block_size(max_block_size_)
        , origin(FileCache::getCommonOrigin())
    {
        auto caches_by_name = FileCacheFactory::instance().getAll();
        for (const auto & [cache_name, cache_data] : caches_by_name)
        {
            unique_caches.insert(cache_data);
            caches_by_instance[cache_data].push_back(cache_name);
        }

        current_cache = unique_caches.begin();
    }

    String getName() const override { return "SystemFilesystemCacheSource"; }

protected:
    Chunk generate() override
    {
        MutableColumnPtr col_cache_name = ColumnString::create();
        MutableColumnPtr col_cache_base_path = ColumnString::create();
        MutableColumnPtr col_path = ColumnString::create();
        MutableColumnPtr col_key = ColumnString::create();
        MutableColumnPtr col_range_begin = ColumnUInt64::create();
        MutableColumnPtr col_range_end = ColumnUInt64::create();
        MutableColumnPtr col_size = ColumnUInt64::create();
        MutableColumnPtr col_state = ColumnString::create();
        MutableColumnPtr col_finished_download_time = ColumnDateTime::create();
        MutableColumnPtr col_hits = ColumnUInt64::create();
        MutableColumnPtr col_references = ColumnUInt64::create();
        MutableColumnPtr col_downloaded_size = ColumnUInt64::create();
        MutableColumnPtr col_kind = ColumnString::create();
        MutableColumnPtr col_unbound = ColumnUInt8::create();
        MutableColumnPtr col_user_id = ColumnString::create();
        MutableColumnPtr col_file_size = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
        MutableColumnPtr col_file_origin = ColumnString::create();

        auto get_total_size = [&] -> size_t
        {
            return col_cache_name->byteSize() +
                col_cache_base_path->byteSize() +
                col_path->byteSize() +
                col_key->byteSize() +
                col_range_begin->byteSize() +
                col_range_end->byteSize() +
                col_size->byteSize() +
                col_state->byteSize() +
                col_finished_download_time->byteSize() +
                col_hits->byteSize() +
                col_references->byteSize() +
                col_downloaded_size->byteSize() +
                col_kind->byteSize() +
                col_unbound->byteSize() +
                col_user_id->byteSize() +
                col_file_origin->byteSize();
        };

        size_t num_rows = 0;
        auto on_file_segment = [&](const FileSegmentInfo & file_segment)
        {
            const auto & cache_data = (*current_cache);
            const auto & cache = cache_data->cache;

            /// There can be several cache names pointing to the same cache object.
            /// We need to add them all to the output.
            for (const auto & cache_name : caches_by_instance.at(cache_data))
            {
                col_cache_name->insert(cache_name);
                col_cache_base_path->insert(cache->getBasePath());

                /// Do not use `file_segment->getPath` here because it will lead to nullptr dereference
                /// (because file_segments in getSnapshot do not have `cache` field set)
                const auto path = cache->getFileSegmentPath(
                    file_segment.key, file_segment.offset, file_segment.kind,
                    file_segment.origin);

                col_path->insert(path);
                col_key->insert(file_segment.key.toString());
                col_range_begin->insert(file_segment.range_left);
                col_range_end->insert(file_segment.range_right);
                col_size->insert(file_segment.size);
                col_state->insert(FileSegment::stateToString(file_segment.state));
                col_finished_download_time->insert(file_segment.download_finished_time);
                col_hits->insert(file_segment.cache_hits);
                col_references->insert(file_segment.references);
                col_downloaded_size->insert(file_segment.downloaded_size);
                col_kind->insert(toString(file_segment.kind));
                col_unbound->insert(file_segment.is_unbound);
                col_user_id->insert(file_segment.origin.user_id);
                col_file_origin->insert(toString(file_segment.origin.segment_type));

                std::error_code ec;
                auto size = fs::file_size(path, ec);
                if (!ec)
                    col_file_size->insert(size);
                else
                    col_file_size->insertDefault();

                ++num_rows;
            }
        };

        while (true)
        {
            if (num_rows && max_block_size && get_total_size() > max_block_size)
                break;

            if (current_cache == unique_caches.end())
                break;

            const auto & cache = (*current_cache)->cache;
            if (!cache->isInitialized())
            {
                ++current_cache;
                current_cache_iterator = nullptr;
                continue;
            }

            if (!current_cache_iterator)
                current_cache_iterator = cache->getCacheIterator(origin.user_id);

            if (!current_cache_iterator->nextBatch(on_file_segment))
            {
                ++current_cache;
                current_cache_iterator = nullptr;
                continue;
            }
        }

        if (!num_rows)
            return {};

        Columns columns{
            std::move(col_cache_name), std::move(col_cache_base_path), std::move(col_path),
            std::move(col_key), std::move(col_range_begin), std::move(col_range_end), std::move(col_size),
            std::move(col_state), std::move(col_finished_download_time), std::move(col_hits),
            std::move(col_references), std::move(col_downloaded_size), std::move(col_kind), std::move(col_unbound),
            std::move(col_user_id), std::move(col_file_origin), std::move(col_file_size)};

        return Chunk(std::move(columns), num_rows);
    }

private:
    const UInt64 max_block_size;
    const FileCacheOriginInfo origin;

    using CachesSet = std::unordered_set<FileCacheFactory::FileCacheDataPtr>;
    CachesSet unique_caches;
    std::unordered_map<FileCacheFactory::FileCacheDataPtr, std::vector<std::string>> caches_by_instance;

    CachesSet::iterator current_cache;
    FileCache::CacheIteratorPtr current_cache_iterator;
};

class ReadFromSystemFilesystemCache final : public SourceStepWithFilter
{
public:
    ReadFromSystemFilesystemCache(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Block & header,
        UInt64 max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(header),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage_limits(query_info.storage_limits)
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "ReadFromSystemFilesystemCache"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto source = std::make_shared<SystemFilesystemCacheSource>(getOutputHeader(), max_block_size, context);
        source->setStorageLimits(storage_limits);
        processors.emplace_back(source);
        pipeline.init(Pipe(std::move(source)));
    }

    /// TODO: void applyFilters(ActionDAGNodes added_filter_nodes) can be implemented to filter out cache names

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
    const UInt64 max_block_size;
};

}

StorageSystemFilesystemCache::StorageSystemFilesystemCache(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"cache_name", std::make_shared<DataTypeString>(), "Name of the cache object. Can be used in `SYSTEM DESCRIBE FILESYSTEM CACHE <name>`, `SYSTEM DROP FILESYSTEM CACHE <name>` commands"},
        {"cache_base_path", std::make_shared<DataTypeString>(), "Path to the base directory where all caches files (of a cache identidied by `cache_name`) are stored."},
        {"cache_path", std::make_shared<DataTypeString>(), "Path to a particular cache file, corresponding to a file segment in a source file"},
        {"key", std::make_shared<DataTypeString>(), "Cache key of the file segment"},
        {"file_segment_range_begin", std::make_shared<DataTypeUInt64>(), "Offset corresponding to the beginning of the file segment range"},
        {"file_segment_range_end", std::make_shared<DataTypeUInt64>(), "Offset corresponding to the (including) end of the file segment range"},
        {"size", std::make_shared<DataTypeUInt64>(), "Size of the file segment"},
        {"state", std::make_shared<DataTypeString>(), "File segment state (DOWNLOADED, DOWNLOADING, PARTIALLY_DOWNLOADED, ...)"},
        {"finished_download_time", std::make_shared<DataTypeDateTime>(), "Time when file segment finished downloading."},
        {"cache_hits", std::make_shared<DataTypeUInt64>(), "Number of cache hits of corresponding file segment"},
        {"references", std::make_shared<DataTypeUInt64>(), "Number of references to corresponding file segment. Value 1 means that nobody uses it at the moment (the only existing reference is in cache storage itself)"},
        {"downloaded_size", std::make_shared<DataTypeUInt64>(), "Downloaded size of the file segment"},
        {"kind", std::make_shared<DataTypeString>(), "File segment kind (used to distringuish between file segments added as a part of 'Temporary data in cache')"},
        {"unbound", std::make_shared<DataTypeNumber<UInt8>>(), "Internal implementation flag"},
        {"user_id", std::make_shared<DataTypeString>(), "User id of the user which created the file segment"},
        {"segment_type", std::make_shared<DataTypeString>(), "Type of the segment. Used to separate data files(`.json`, `.txt` and etc) from data file(`.bin`, mark files)."},
        {"file_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "File size of the file to which current file segment belongs"},
    }));
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemFilesystemCache::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtualsList());
    auto read_step = std::make_unique<ReadFromSystemFilesystemCache>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        header,
        max_block_size);
    query_plan.addStep(std::move(read_step));
}

}
