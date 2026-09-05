#include <Processors/Transforms/ExternalDistinctTransform.h>

#include <algorithm>
#include <iterator>

#include <Interpreters/sortBlock.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/SortingTransform.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>

namespace ProfileEvents
{
    extern const Event ExternalDistinctMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{

/// A run is written only when at least this much data was accumulated (but never more than the spill
/// threshold itself, so that tiny thresholds still spill deterministically). Without a floor, when it is
/// some *other* operator that keeps the memory usage of the query above the threshold, every consumed
/// chunk would be dumped as its own temporary file.
constexpr size_t MIN_BYTES_IN_RUN = DEFAULT_BLOCK_SIZE * 256;

/// Deduplicates merged runs with `DistinctSortedFilter` and removes the emitted flag column.
class MergedRunsDistinctTransform final : public ISimpleTransform
{
public:
    MergedRunsDistinctTransform(
        SharedHeader input_header,
        SharedHeader output_header,
        ColumnNumbers key_columns_pos,
        SortDescription description,
        size_t flag_column_pos)
        : ISimpleTransform(std::move(input_header), std::move(output_header), /*skip_empty_chunks_=*/ true)
        , filter(std::move(key_columns_pos), std::move(description), flag_column_pos)
    {
    }

    String getName() const override { return "MergedRunsDistinctTransform"; }

protected:
    void transform(Chunk & chunk) override { chunk = filter.filter(std::move(chunk), /*strip_flag=*/ true); }

private:
    DistinctSortedFilter filter;
};

}

ExternalDistinctTransform::ExternalDistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    size_t max_bytes_before_external_distinct_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t min_free_disk_space_,
    size_t max_block_size_rows_,
    bool preserve_input_order_)
    : IProcessor({header_}, {header_})
    , distinct_set(*header_, columns_, set_size_limits_, /*skip_null_keys_=*/ false, /*require_extractable_keys_=*/ true)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , max_bytes_before_external_distinct(max_bytes_before_external_distinct_)
    , tmp_data(std::move(tmp_data_))
    , min_free_disk_space(min_free_disk_space_)
    , max_block_size_rows(max_block_size_rows_)
    , spill_layout(header_, distinct_set.getKeyColumnsPositions(), preserve_input_order_)
    , run_dedup(
          spill_layout.getKeyColumnsPositions(), spill_layout.getKeySortDescription(), spill_layout.getFlagColumnPosition())
{
    chassert(max_bytes_before_external_distinct > 0);
    /// `DistinctStep` selects this transform only when the distinct key has non-constant columns.
    chassert(distinct_set.hasKeyColumns());
}

ExternalDistinctTransform::~ExternalDistinctTransform() = default;

size_t ExternalDistinctTransform::minBytesInRun() const
{
    return std::min(max_bytes_before_external_distinct, MIN_BYTES_IN_RUN);
}

Chunk ExternalDistinctTransform::sortSpillChunk(Chunk chunk, bool already_emitted) const
{
    /// Stable sorting retains the first-arriving payload and the first binary representation among
    /// keys that compare equal. The service columns follow the same permutation as the input columns.
    Block block = spill_layout.getSpillHeader()->cloneWithColumns(chunk.detachColumns());
    if (already_emitted)
        sortBlock(block, spill_layout.getKeySortDescription(), /*limit=*/ 0, IColumn::PermutationSortStability::Stable);
    else
        sortBlockAndDeduplicate(block, spill_layout.getKeySortDescription(), IColumn::PermutationSortStability::Stable);

    return Chunk(block.getColumns(), block.rows());
}

void ExternalDistinctTransform::startFirstSpill()
{
    spilled = true;

    LOG_TRACE(log, "Switching DISTINCT to the external mode (query memory: {}, limit: {})",
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    Chunks run_chunks;
    size_t run_bytes = 0;
    /// Suppression rows need only the extracted keys. Release the set before sorting, so the transient
    /// peak contains the set and raw keys without also retaining their sorted copies.
    auto key_batches = distinct_set.extractKeyColumns(max_block_size_rows);
    distinct_set.clear();

    for (auto & key_columns : key_batches)
    {
        auto prepared = sortSpillChunk(
            spill_layout.prepareSuppressionChunk(std::move(key_columns)), /*already_emitted=*/ true);
        run_bytes += prepared.allocatedBytes();
        run_chunks.push_back(std::move(prepared));
    }

    startSpillRun(std::move(run_chunks), run_bytes, /*is_first_run=*/ true);
}

void ExternalDistinctTransform::startSpillRun(Chunks run_chunks, size_t run_bytes, bool is_first_run)
{
    const auto & spill_header = spill_layout.getSpillHeader();
    const auto & merged_header = spill_layout.getMergedHeader();
    const auto & description = spill_layout.getKeySortDescription();

    if (!tmp_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDisk is not set for ExternalDistinctTransform");
    ++temporary_files_num;

    LOG_TRACE(log, "Will dump distinct run ({} chunks, {}) to disk (query memory: {}, limit: {})",
        run_chunks.size(),
        formatReadableSizeWithBinarySuffix(run_bytes),
        formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
        formatReadableSizeWithBinarySuffix(max_bytes_before_external_distinct));

    /// If there's less free disk space than reserve_size, an exception will be thrown.
    const size_t reserve_size = run_bytes + min_free_disk_space;
    TemporaryBlockStreamHolder tmp_stream(spill_header, tmp_data, reserve_size);

    current_run_is_deduplicated = is_first_run || run_chunks.size() == 1;
    if (!current_run_is_deduplicated)
        run_dedup.reset();

    /// The limit hint cannot be applied inside the sort or the merge: rows are suppressed by the
    /// deduplication after them, so cutting the streams at `limit_hint` rows could lose distinct values.
    merge_sorter = std::make_unique<MergeSorter>(spill_header, std::move(run_chunks), description, max_block_size_rows, /*limit=*/ 0);

    auto sink = std::make_shared<BufferingToFileSink>(spill_header, std::move(tmp_stream), log);
    auto source = std::make_shared<BufferingFromFileSource>(spill_header, sink->getHolder(), log);

    processors.emplace_back(source);
    processors.emplace_back(sink);

    if (!external_merging_sorted)
    {
        external_merging_sorted = std::make_shared<MergingSortedTransform>(
            spill_header,
            /*num_inputs=*/ 0,
            description,
            max_block_size_rows,
            /*max_block_size_bytes=*/ 0,
            /*max_dynamic_subcolumns=*/ std::nullopt,
            SortingQueueStrategy::Batch,
            /*limit_=*/ 0,
            /*always_read_till_end_=*/ false,
            /*out_row_sources_buf_=*/ nullptr,
            /*filter_column_name_=*/ std::nullopt,
            /*use_average_block_sizes=*/ false,
            /*apply_virtual_row_conversions=*/ false,
            /*virtual_row_prefetch_window=*/ 0,
            /*have_all_inputs_=*/ false);
        processors.emplace_back(external_merging_sorted);

        merged_stream_processors.emplace_back(std::make_shared<MergedRunsDistinctTransform>(
            spill_header, merged_header, spill_layout.getKeyColumnsPositions(), description, spill_layout.getFlagColumnPosition()));

        if (spill_layout.preservesInputOrder())
        {
            const auto & arrival_number_description = spill_layout.getArrivalNumberSortDescription();

            /// The merge returns the rows in DISTINCT-key order; sort them back by their arrival numbers:
            /// each chunk on its own first, then a merge of the sorted chunks, which spills under the same
            /// conditions as the runs are written. The limit hint bounds the sort: the rows it cuts off
            /// would not be emitted anyway.
            merged_stream_processors.emplace_back(
                std::make_shared<PartialSortingTransform>(merged_header, arrival_number_description, limit_hint));
            merged_stream_processors.emplace_back(std::make_shared<MergeSortingTransform>(
                merged_header,
                arrival_number_description,
                max_block_size_rows,
                /*max_block_bytes=*/ 0,
                limit_hint,
                /*increase_sort_description_compile_attempts=*/ false,
                /*max_bytes_before_remerge_=*/ 0,
                /*remerge_lowered_memory_bytes_ratio_=*/ 0.,
                minBytesInRun(),
                max_bytes_before_external_distinct,
                tmp_data,
                min_free_disk_space));
        }

        for (const auto & processor : merged_stream_processors)
            processors.emplace_back(processor);
    }

    stage = Stage::Serialize;
    sum_bytes_in_chunks = 0;
}

IProcessor::PipelineUpdate ExternalDistinctTransform::updatePipeline()
{
    if (processors.size() > 2)
    {
        /// The first spill: the merged stream of the runs passes through its stages (see
        /// merged_stream_processors) and comes back through a new input port.
        auto * output = &external_merging_sorted->getOutputs().front();
        for (const auto & processor : merged_stream_processors)
        {
            connect(*output, processor->getInputs().front());
            output = &processor->getOutputs().front();
        }

        inputs.emplace_back(*spill_layout.getMergedHeader(), this);
        connect(*output, inputs.back());
    }

    auto & source = processors.front();

    static_cast<MergingSortedTransform &>(*external_merging_sorted).addInput();
    connect(source->getOutputs().back(), external_merging_sorted->getInputs().back());

    if (processors.size() > 1)
    {
        auto & sink = *std::next(processors.begin());
        /// Serialize: the run flows out through a new output port into the sink.
        outputs.emplace_back(*spill_layout.getSpillHeader(), this);
        connect(sink->getOutputs().front(), source->getInputs().front());
        connect(getOutputs().back(), sink->getInputs().back());
    }
    else
    {
        /// Generate: the leftover in-memory chunks were added as the last input of the merge.
        static_cast<MergingSortedTransform &>(*external_merging_sorted).setHaveAllInputs();
    }

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

IProcessor::Status ExternalDistinctTransform::prepare()
{
    if (stage == Stage::Serialize)
    {
        if (!processors.empty())
            return Status::UpdatePipeline;

        auto status = prepareSerialize();
        if (status != Status::Finished)
            return status;

        stage = Stage::Consume;
    }

    if (stage == Stage::Consume)
    {
        auto status = prepareConsume();
        if (status != Status::Finished)
            return status;

        stage = Stage::Generate;
    }

    /// stage == Stage::Generate

    if (!generated_prefix)
        return Status::Ready;

    if (!processors.empty())
        return Status::UpdatePipeline;

    return prepareGenerate();
}

IProcessor::Status ExternalDistinctTransform::prepareConsume()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    if (read_stopped)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!current_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        current_chunk = input.pull(true);
    }

    /// Now consume.
    return Status::Ready;
}

IProcessor::Status ExternalDistinctTransform::prepareSerialize()
{
    auto & output = outputs.back();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (current_chunk)
        output.push(std::move(current_chunk));

    if (merge_sorter)
        return Status::Ready;

    output.finish();
    return Status::Finished;
}

IProcessor::Status ExternalDistinctTransform::prepareGenerate()
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

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    /// Nothing was spilled - everything was already streamed downstream during the Consume stage.
    if (temporary_files_num == 0)
    {
        output.finish();
        return Status::Finished;
    }

    if (read_stopped)
    {
        for (auto & input : inputs)
            input.close();

        output.finish();
        return Status::Finished;
    }

    /// The port through which the merged stream of the spilled runs comes back.
    auto & input = inputs.back();

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    /// The deduplication of the merged chunk is real work, so it belongs to work().
    return Status::Ready;
}

void ExternalDistinctTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));

    if (stage == Stage::Serialize)
        serialize();

    if (stage == Stage::Generate)
        generate();
}

void ExternalDistinctTransform::consume(Chunk chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    const UInt64 first_arrival_number = consumed_rows;
    consumed_rows += chunk.getNumRows();

    if (!spilled)
    {
        Chunk filtered = distinct_set.filter(std::move(chunk));
        if (filtered.hasRows())
        {
            emitted_rows += filtered.getNumRows();
            generated_chunk = std::move(filtered);

            if (limit_hint && emitted_rows >= limit_hint)
            {
                read_stopped = true;
                return;
            }
        }

        /// A size limit with the 'break' overflow mode was reached: the partial chunk above is still
        /// emitted, and no further input can produce output.
        if (distinct_set.isLimitReached())
        {
            read_stopped = true;
            return;
        }

        if (distinct_set.getTotalRowCount() > 0 && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct))
            startFirstSpill();
    }
    else
    {
        auto prepared = sortSpillChunk(
            spill_layout.prepareInputChunk(std::move(chunk), first_arrival_number), /*already_emitted=*/ false);
        sum_bytes_in_chunks += prepared.allocatedBytes();
        chunks.push_back(std::move(prepared));

        /// The floor on the run size prevents dumping every chunk as its own file when it is another
        /// operator that keeps the memory usage of the query above the threshold.
        if (sum_bytes_in_chunks >= minBytesInRun()
            && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_distinct))
        {
            auto run_chunks = std::move(chunks);
            chunks.clear();
            startSpillRun(std::move(run_chunks), sum_bytes_in_chunks, /*is_first_run=*/ false);
        }
    }
}

void ExternalDistinctTransform::serialize()
{
    /// The loop can process many blocks in one call when the deduplication filters whole blocks out
    /// (heavily duplicated runs), so check for cancellation: ending the run stream early is harmless
    /// when the pipeline is being torn down anyway.
    while (!isCancelled())
    {
        current_chunk = merge_sorter->read();
        if (!current_chunk)
            break;

        if (current_run_is_deduplicated)
            return;

        /// Local deduplication of the run. Pushing an empty chunk would end the temporary file stream
        /// prematurely, so fully filtered out chunks are skipped.
        current_chunk = run_dedup.filter(std::move(current_chunk), /*strip_flag=*/ false);
        if (current_chunk.hasRows())
            return;
    }

    merge_sorter.reset();
}

void ExternalDistinctTransform::generate()
{
    if (!generated_prefix)
    {
        generated_prefix = true;

        if (temporary_files_num > 0)
        {
            ProfileEvents::increment(ProfileEvents::ExternalDistinctMerge);
            LOG_INFO(log, "There are {} temporary distinct runs to merge", temporary_files_num);

            /// The leftover in-memory chunks are the last input of the merge. They are not locally
            /// deduplicated: the merge-phase deduplication collapses binary-equal rows within one input
            /// just as well.
            processors.emplace_back(std::make_shared<MergeSorterSource>(
                spill_layout.getSpillHeader(), std::move(chunks), spill_layout.getKeySortDescription(),
                max_block_size_rows, /*limit=*/ 0));
        }

        return;
    }

    if (!current_chunk || !current_chunk.hasRows())
        return;

    /// The chunk is merged, deduplicated and, when the input order is preserved, sorted back by the
    /// arrival numbers, which have done their job by now.
    Chunk chunk = spill_layout.restoreOutputChunk(std::move(current_chunk));

    emitted_rows += chunk.getNumRows();
    generated_chunk = std::move(chunk);

    /// Post-spill the hash set does not exist anymore. The rows limit stays exact: the number of the
    /// emitted rows is precisely the number of distinct values. The bytes limit restricts the in-memory
    /// state of the set, which is bounded by the spilling itself, so it has nothing left to check.
    if ((limit_hint && emitted_rows >= limit_hint)
        || !set_size_limits.check(emitted_rows, /*bytes=*/ 0, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        read_stopped = true;
}

}
