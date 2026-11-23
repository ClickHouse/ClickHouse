#include <Storages/StorageFilesystemCache.h>

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageSnapshot.h>
#include <Common/assert_cast.h>

#include <magic_enum.hpp>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void insertValue(IColumn & column, UInt64 value) { assert_cast<ColumnUInt64 &>(column).insertValue(value); }

void insertValue(IColumn & column, std::string_view value) { assert_cast<ColumnString &>(column).insertData(value.data(), value.size()); }

void insertValue(IColumn & column, FileSegmentState state) { insertValue(column, std::string_view(FileSegment::stateToString(state))); }

/// `NULL` for `None`, the name of the queue otherwise.
template <typename Queue>
requires std::is_enum_v<Queue>
void insertValue(IColumn & column, Queue queue)
{
    if (queue == Queue::None)
    {
        column.insertDefault();
        return;
    }
    const auto name = magic_enum::enum_name(queue);
    assert_cast<ColumnNullable &>(column).insertData(name.data(), name.size());
}

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
            auto fill = [&](auto FileSegmentSummaryInfo::* member)
            {
                for (size_t row = begin; row < end; ++row)
                    insertValue(column, (*infos)[row].*member);
            };

            const auto & name = header.getByPosition(i).name;
            if (name == "priority_rank")
                fill(&FileSegmentSummaryInfo::priority_rank);
            else if (name == "path")
                fill(&FileSegmentSummaryInfo::path);
            else if (name == "key")
                fill(&FileSegmentSummaryInfo::key);
            else if (name == "offset")
                fill(&FileSegmentSummaryInfo::offset);
            else if (name == "size")
                fill(&FileSegmentSummaryInfo::size);
            else if (name == "state")
                fill(&FileSegmentSummaryInfo::state);
            else if (name == "split_queue")
                fill(&FileSegmentSummaryInfo::split_queue);
            else if (name == "slru_queue")
                fill(&FileSegmentSummaryInfo::slru_queue);
            else if (name == "user_id")
                fill(&FileSegmentSummaryInfo::user_id);
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
         "1-based position of the segment in the output for 'hottest' and 'coldest', i.e. the priority order. "
         "For 'random': distinct, but in no particular order."},
        {"path", std::make_shared<DataTypeString>(), "The file of the segment on disk, including the `_<size>` suffix of downloaded segments."},
        {"key", std::make_shared<DataTypeString>(), "Cache key."},
        {"offset", std::make_shared<DataTypeUInt64>(), "Offset of the segment in the source file."},
        {"size", std::make_shared<DataTypeUInt64>(), "Size of the segment as accounted by the cache."},
        {"state", std::make_shared<DataTypeString>(), "Download state of the segment, e.g. `DOWNLOADED`."},
        {"split_queue", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
         "The queue of the segment in a cache with `use_split_cache`: `Data` or `System`, the same as `system.filesystem_cache.segment_type`. "
         "`NULL` if the cache is not split."},
        {"slru_queue", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
         "The queue of the segment in a cache with the SLRU policy: `Protected` or `Probationary`. `NULL` if the policy is not SLRU."},
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
