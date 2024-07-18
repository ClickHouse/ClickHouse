#include <Storages/System/StorageSystemDetachedParts.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/threadPoolCallbackRunner.h>

#include <mutex>

namespace DB
{

namespace
{

void calculateTotalSizeOnDiskImpl(const DiskPtr & disk, const String & from, UInt64 & total_size)
{
    /// Files or directories of detached part may not exist. Only count the size of existing files.
    if (disk->isFile(from))
    {
        total_size += disk->getFileSize(from);
    }
    else
    {
        for (auto it = disk->iterateDirectory(from); it->isValid(); it->next())
            calculateTotalSizeOnDiskImpl(disk, fs::path(from) / it->name(), total_size);
    }
}

UInt64 calculateTotalSizeOnDisk(const DiskPtr & disk, const String & from)
{
    UInt64 total_size = 0;
    try
    {
        calculateTotalSizeOnDiskImpl(disk, from, total_size);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    return total_size;
}

class SourceState
{
    std::mutex mutex;
    StoragesInfoStream stream;

public:
    explicit SourceState(StoragesInfoStream && stream_)
        : stream(std::move(stream_))
    {}

    StoragesInfo next()
    {
        std::lock_guard lock(mutex);
        return stream.next();
    }
};


struct WorkerState
{
    struct Task
    {
        DiskPtr disk;
        String path;
        std::atomic<size_t> * counter = nullptr;
    };

    std::vector<Task> tasks;
    std::atomic<size_t> next_task = {0};
};

class DetachedPartsSource : public ISource
{
public:
    DetachedPartsSource(Block header_, std::shared_ptr<SourceState> state_, std::vector<UInt8> columns_mask_, UInt64 block_size_)
        : ISource(std::move(header_))
        , state(state_)
        , columns_mask(std::move(columns_mask_))
        , block_size(block_size_)
    {}

    String getName() const override { return "DataPartsSource"; }

protected:
    static Chunk nullWhenNoRows(MutableColumns && new_columns)
    {
        chassert(!new_columns.empty());
        const auto rows = new_columns[0]->size();

        if (!rows)
            return {};

        return {std::move(new_columns), rows};
    }

    Chunk generate() override
    {
        MutableColumns new_columns = getPort().getHeader().cloneEmptyColumns();
        chassert(!new_columns.empty());

        while (new_columns[0]->size() < block_size)
        {
            if (detached_parts.empty())
                getMoreParts();

            if (detached_parts.empty())
                return nullWhenNoRows(std::move(new_columns));

            generateRows(new_columns, block_size - new_columns[0]->size());
        }

        return nullWhenNoRows(std::move(new_columns));
    }

private:
    std::shared_ptr<SourceState> state;
    const std::vector<UInt8> columns_mask;
    const UInt64 block_size;
    const size_t support_threads = 35;

    StoragesInfo current_info;
    DetachedPartsInfo detached_parts;

    void getMoreParts()
    {
        chassert(detached_parts.empty());

        while (detached_parts.empty())
        {
            current_info = state->next();
            if (!current_info)
                return;

            detached_parts = current_info.data->getDetachedParts();
        }
    }

    void calculatePartSizeOnDisk(size_t begin, std::vector<std::atomic<size_t>> & parts_sizes)
    {
        WorkerState worker_state;

        for (auto p_id = begin; p_id < detached_parts.size(); ++p_id)
        {
            auto & part = detached_parts[p_id];
            auto part_path = fs::path(MergeTreeData::DETACHED_DIR_NAME) / part.dir_name;
            auto relative_path = fs::path(current_info.data->getRelativeDataPath()) / part_path;
            worker_state.tasks.push_back({part.disk, relative_path, &parts_sizes.at(p_id - begin)});
        }

        std::vector<std::future<void>> futures;
        SCOPE_EXIT_SAFE({
            /// Cancel all workers
            worker_state.next_task.store(worker_state.tasks.size());
            /// Exceptions are not propagated
            for (auto & future : futures)
                if (future.valid())
                    future.wait();
            futures.clear();
        });

        auto max_thread_to_run = std::max(size_t(1), std::min(support_threads, worker_state.tasks.size() / 10));
        futures.reserve(max_thread_to_run);

        for (size_t i = 0; i < max_thread_to_run; ++i)
        {
            if (worker_state.next_task.load() >= worker_state.tasks.size())
                break;

            auto worker = [&worker_state] ()
            {
                for (auto id = worker_state.next_task++; id < worker_state.tasks.size(); id = worker_state.next_task++)
                {
                    auto & task = worker_state.tasks.at(id);
                    size_t size = calculateTotalSizeOnDisk(task.disk, task.path);
                    task.counter->store(size);
                }
            };

            futures.push_back(
                        scheduleFromThreadPool<void>(
                            std::move(worker),
                            getIOThreadPool().get(),
                            "DP_BytesOnDisk"));
        }

        /// Exceptions are propagated
        for (auto & future : futures)
            future.get();
    }

    void generateRows(MutableColumns & new_columns, size_t max_rows)
    {
        chassert(current_info);

        auto rows = std::min(max_rows, detached_parts.size());
        auto begin = detached_parts.size() - rows;

        std::vector<std::atomic<size_t>> parts_sizes(rows);
        constexpr size_t bytes_on_disk_col_idx = 4;
        if (columns_mask[bytes_on_disk_col_idx])
            calculatePartSizeOnDisk(begin, parts_sizes);

        for (auto p_id = begin; p_id < detached_parts.size(); ++p_id)
        {
            auto & p = detached_parts.at(p_id);

            size_t src_index = 0;
            size_t res_index = 0;
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(current_info.database);
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(current_info.table);
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.partition_id : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.dir_name);
            if (columns_mask[src_index++])
            {
                chassert(src_index - 1 == bytes_on_disk_col_idx);
                size_t bytes_on_disk = parts_sizes.at(p_id - begin).load();
                new_columns[res_index++]->insert(bytes_on_disk);
            }
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.disk->getName());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert((fs::path(current_info.data->getFullPathOnDisk(p.disk)) / MergeTreeData::DETACHED_DIR_NAME / p.dir_name).string());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.prefix : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.min_block : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.max_block : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.level : Field());
        }

        detached_parts.resize(begin);
    }
};

}

StorageSystemDetachedParts::StorageSystemDetachedParts(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{{
        {"database",         std::make_shared<DataTypeString>()},
        {"table",            std::make_shared<DataTypeString>()},
        {"partition_id",     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"name",             std::make_shared<DataTypeString>()},
        {"bytes_on_disk",    std::make_shared<DataTypeUInt64>()},
        {"disk",             std::make_shared<DataTypeString>()},
        {"path",             std::make_shared<DataTypeString>()},
        {"reason",           std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"min_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"level",            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())}
    }});
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemDetachedParts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t num_streams)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, header] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    auto state = std::make_shared<SourceState>(StoragesInfoStream(query_info, context));

    Pipe pipe;

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<DetachedPartsSource>(header.cloneEmpty(), state, columns_mask, max_block_size);
        pipe.addSource(std::move(source));
    }

    return pipe;
}

}
