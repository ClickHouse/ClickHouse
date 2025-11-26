#include <Processors/Sources/LazyReadFromMergeTreeSource.h>
#include <Processors/QueryPlan/LazyReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 preferred_max_column_in_block_size_bytes;
    extern const SettingsBool merge_tree_use_const_size_tasks_for_remote_reading;
    extern const SettingsBool use_uncompressed_cache;
}

LazyReadFromMergeTreeSource::LazyReadFromMergeTreeSource(
    SharedHeader header,
    size_t max_block_size_,
    size_t max_threads_,
    ExpressionActionsSettings actions_settings_,
    MergeTreeReaderSettings reader_settings_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    const std::string & log_name_,
    LazyMaterializingRowsPtr lazy_materializing_rows_)
    : IProcessor({}, {std::move(header)})
    , max_block_size(max_block_size_)
    , max_threads(max_threads_)
    , actions_settings(actions_settings_)
    , reader_settings(reader_settings_)
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , log_name(log_name_)
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
}

LazyReadFromMergeTreeSource::~LazyReadFromMergeTreeSource() = default;

IProcessor::Status LazyReadFromMergeTreeSource::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (lazy_materializing_rows)
        return Status::ExpandPipeline;

    while (next_input_to_process != inputs.end())
    {
        // std::cerr << "LazyReadFromMergeTreeSource: " << std::distance(inputs.begin(), next_input_to_process) << " of " << inputs.size() << " inputs\n";
        auto & input = *next_input_to_process;
        if (input.isFinished())
        {
            next_input_to_process++;
            continue;
        }

        if (!input.hasData())
            return Status::NeedData;

        auto chunk = input.pull();
        // std::cerr << "Pulled chunk with " << chunk.getNumRows() << " rows\n";
        output.push(std::move(chunk));

        return Status::PortFull;
    }

    output.finish();
    return Status::Finished;
}

Processors LazyReadFromMergeTreeSource::expandPipeline()
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyReadFromMergeTreeSource: No lazy materializing rows");
    auto processors = buildReaders();
    lazy_materializing_rows.reset();
    for (auto & processor : processors)
    {
        auto & output = processor->getOutputs().front();
        inputs.emplace_back(output.getHeader(), this);
        connect(output, inputs.back());
        inputs.back().setNeeded();
    }

    next_input_to_process = inputs.begin();
    return processors;
}

Processors LazyReadFromMergeTreeSource::buildReaders()
{
    const auto & ctx_settings = context->getSettingsRef();
    size_t sum_marks = lazy_materializing_rows->ranges_in_data_parts.getMarksCountAllParts();

    MergeTreeReadPoolBase::PoolSettings pool_settings{
        .threads = max_threads,
        .sum_marks = sum_marks,
        .min_marks_for_concurrent_read = /*min_marks_for_concurrent_read*/ 1,
        .preferred_block_size_bytes = ctx_settings[Setting::preferred_block_size_bytes],
        .use_uncompressed_cache = ctx_settings[Setting::use_uncompressed_cache],
        .use_const_size_tasks_for_remote_reading = ctx_settings[Setting::merge_tree_use_const_size_tasks_for_remote_reading],
        .total_query_nodes = 1,
    };

    MergeTreeReadTask::BlockSizeParams block_size{
        .max_block_size_rows = max_block_size,
        .preferred_block_size_bytes = ctx_settings[Setting::preferred_block_size_bytes],
        .preferred_max_column_in_block_size_bytes = ctx_settings[Setting::preferred_max_column_in_block_size_bytes]};

    /// Why this is needed?
    VirtualFields shared_virtual_fields;
    shared_virtual_fields.emplace("_sample_factor", 1.0);

    auto pool = std::make_shared<MergeTreeReadPoolInOrder>(
        false,
        MergeTreeReadType::InOrder,
        lazy_materializing_rows->ranges_in_data_parts,
        mutations_snapshot,
        shared_virtual_fields,
        /*index_read_tasks*/ IndexReadTasks{},
        storage_snapshot,
        /* row_level_filter */ nullptr,
        /* prewhere_info */ nullptr,
        actions_settings,
        reader_settings,
        outputs.front().getHeader().getNames(),
        pool_settings,
        block_size,
        context);

    // std::cerr << "LazyReadFromMergeTreeSource parts: " << lazy_materializing_rows->ranges_in_data_parts.size() << " readers\n";
    // std::cerr << lazy_materializing_rows->rows_in_parts.size() << " parts with rows\n";
    // for (size_t i = 0; i < lazy_materializing_rows->rows_in_parts.size(); ++i)
    // {
    //     auto idx = lazy_materializing_rows->ranges_in_data_parts[i].part_index_in_query;
    //     // std::cerr << "part index " << idx << "\n";
    //     const auto & rows = lazy_materializing_rows->rows_in_parts[idx];
    //     std::cerr << "rows " << rows.size() << "\n";
    //     for (auto row : rows)
    //         std::cerr << row << " ";
    //     std::cerr << "\n";
    // }

    Processors processors;
    for (size_t i = 0; i < lazy_materializing_rows->ranges_in_data_parts.size(); ++i)
    {
        //const auto & part_with_ranges = lazy_materializing_rows->ranges_in_data_parts[i];

        MergeTreeSelectAlgorithmPtr algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool,
            std::move(algorithm),
            nullptr,
            nullptr,
            /*lazily_read_info*/ nullptr,
            /*index_read_tasks*/ IndexReadTasks{},
            actions_settings,
            reader_settings,
            /*index_build_context*/ nullptr,
            lazy_materializing_rows);

        //processor->addPartLevelToChunk(isQueryWithFinal());

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), log_name);
        // if (set_total_rows_approx)
        //     source->addTotalRowsApprox(total_rows);

        processors.emplace_back(std::move(source));
    }
    return processors;
}

}
