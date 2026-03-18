#include <Storages/System/StorageSystemDetachedParts.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <IO/SharedThreadPools.h>
#include <Common/threadPoolCallbackRunner.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <mutex>

namespace DB
{

namespace
{

void calculateTotalSizeOnDiskImpl(const DiskPtr & disk, const String & from, UInt64 & total_size)
{
    /// Files or directories of detached part may not exist. Only count the size of existing files.
    if (disk->existsFile(from))
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

        auto max_thread_to_run = std::max(size_t(1), std::min(support_threads, worker_state.tasks.size() / 10));

        ThreadPoolCallbackRunnerLocal<void> runner(getIOThreadPool().get(), "DP_BytesOnDisk");

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

            runner(std::move(worker));
        }

        runner.waitForAllToFinishAndRethrowFirstError();
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
            {
                Poco::Timestamp modification_time{};
                try
                {
                    modification_time = p.disk->getLastModified(fs::path(current_info.data->getRelativeDataPath()) / MergeTreeData::DETACHED_DIR_NAME / p.dir_name);
                }
                catch (const fs::filesystem_error &)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
                new_columns[res_index++]->insert(static_cast<UInt64>(modification_time.epochTime()));
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
        {"database",         std::make_shared<DataTypeString>(), "The name of the database this part belongs to."},
        {"table",            std::make_shared<DataTypeString>(), "The name of the table this part belongs to."},
        {"partition_id",     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The identifier of the partition this part belongs to."},
        {"name",             std::make_shared<DataTypeString>(), "The name of the part."},
        {"bytes_on_disk",    std::make_shared<DataTypeUInt64>(), "Total size of all the data part files in bytes."},
        {"modification_time",std::make_shared<DataTypeDateTime>(), "The time the directory with the data part was modified. This usually corresponds to the time when detach happened."},
        {"disk",             std::make_shared<DataTypeString>(), "The name of the disk that stores this data part."},
        {"path",             std::make_shared<DataTypeString>(), "The path of the disk to the file of this data part."},
        {"reason",           std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The explanation why this part was detached."},
        {"min_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()), "The minimum number of data parts that make up the current part after merging."},
        {"max_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()), "The maximum number of data parts that make up the current part after merging."},
        {"level",            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()), "Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts."},
    }});
    setInMemoryMetadata(storage_metadata);
}

class ReadFromSystemDetachedParts : public SourceStepWithFilter
{
public:
    ReadFromSystemDetachedParts(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<StorageSystemDetachedParts> storage_,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {}

    std::string getName() const override { return "ReadFromSystemDetachedParts"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

protected:
    std::shared_ptr<StorageSystemDetachedParts> storage;
    std::vector<UInt8> columns_mask;

    std::optional<ActionsDAG> filter;
    const size_t max_block_size;
    const size_t num_streams;
};

void ReadFromSystemDetachedParts::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        const auto * predicate = filter_actions_dag->getOutputs().at(0);

        Block block;
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), "database"));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), "table"));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), "engine"));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeUInt8>(), "active"));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeUUID>(), "uuid"));

        filter = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &block);
        if (filter)
            VirtualColumnUtils::buildSetsForDAG(*filter, context);
    }
}

void StorageSystemDetachedParts::read(
    QueryPlan & query_plan,
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

    auto this_ptr = std::static_pointer_cast<StorageSystemDetachedParts>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemDetachedParts>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(this_ptr), std::move(columns_mask), max_block_size, num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemDetachedParts::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto state = std::make_shared<SourceState>(StoragesInfoStream({}, std::move(filter), context));

    Pipe pipe;

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<DetachedPartsSource>(getOutputHeader(), state, columns_mask, max_block_size);
        pipe.addSource(std::move(source));
    }

    pipeline.init(std::move(pipe));
}

}
