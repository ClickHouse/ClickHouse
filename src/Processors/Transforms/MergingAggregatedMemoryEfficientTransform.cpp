#include <limits>
#include <Interpreters/Aggregator.h>
#include <Interpreters/sortBlock.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GroupingAggregatedTransform::GroupingAggregatedTransform(
    const Block & header_, size_t num_inputs_, AggregatingTransformParamsPtr params_)
    : IProcessor(InputPorts(num_inputs_, header_), { Block() })
    , num_inputs(num_inputs_)
    , params(std::move(params_))
    , last_bucket_number(num_inputs, -1)
{
}

void GroupingAggregatedTransform::pushData(Chunks chunks, Int32 bucket, bool is_overflows)
{
    auto & output = outputs.front();

    auto info = std::make_shared<ChunksToMerge>();
    info->bucket_num = bucket;
    info->is_overflows = is_overflows;
    info->chunks = std::make_shared<Chunks>(std::move(chunks));

    Chunk chunk;
    chunk.getChunkInfos().add(std::move(info));
    output.push(std::move(chunk));
}

bool GroupingAggregatedTransform::tryPushTwoLevelData()
{
    auto try_push_by_iter = [&](auto batch_it)
    {
        if (batch_it == chunks_map.end())
            return false;

        Chunks & cur_chunks = batch_it->second;
        if (cur_chunks.empty())
        {
            chunks_map.erase(batch_it);
            return false;
        }

        pushData(std::move(cur_chunks), batch_it->first, false);
        chunks_map.erase(batch_it);
        return true;
    };

    if (all_inputs_finished)
    {
        /// Chunks are sorted by bucket.
        while (!chunks_map.empty())
            if (try_push_by_iter(chunks_map.begin()))
                return true;
    }
    else
    {
        for (; next_bucket_to_push < current_bucket; ++next_bucket_to_push)
            if (try_push_by_iter(chunks_map.find(next_bucket_to_push)))
                return true;
    }

    return false;
}

bool GroupingAggregatedTransform::tryPushSingleLevelData()
{
    if (single_level_chunks.empty())
        return false;

    pushData(std::move(single_level_chunks), -1, false);
    return true;
}

bool GroupingAggregatedTransform::tryPushOverflowData()
{
    if (overflow_chunks.empty())
        return false;

    pushData(std::move(overflow_chunks), -1, true);
    return true;
}

IProcessor::Status GroupingAggregatedTransform::prepare(const PortNumbers & updated_input_ports, const PortNumbers &)
{
    /// Check can output.
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        chunks_map.clear();
        last_bucket_number.clear();
        return Status::Finished;
    }

    if (!initialized_index_to_input)
    {
        initialized_index_to_input = true;
        auto in = inputs.begin();
        index_to_input.resize(num_inputs);

        for (size_t i = 0; i < num_inputs; ++i, ++in)
            index_to_input[i] = in;
    }

    auto need_input = [this](size_t input_num)
    {
        if (last_bucket_number[input_num] < current_bucket)
            return true;

        return expect_several_chunks_for_single_bucket_per_source && last_bucket_number[input_num] == current_bucket;
    };

    if (!wait_input_ports_numbers.empty())
    {
        for (const auto & updated_input_port_number : updated_input_ports)
        {
            if (!wait_input_ports_numbers.contains(updated_input_port_number))
                continue;

            auto & input = index_to_input[updated_input_port_number];
            if (!input->hasData())
            {
                wait_input_ports_numbers.erase(updated_input_port_number);
                continue;
            }

            auto chunk = input->pull();
            addChunk(std::move(chunk), updated_input_port_number);

            if (!input->isFinished() && need_input(updated_input_port_number))
                continue;

            wait_input_ports_numbers.erase(updated_input_port_number);
        }

        if (!wait_input_ports_numbers.empty())
            return Status::NeedData;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();

        return Status::PortFull;
    }

    /// Convert single level to two levels if have two-level input.
    if (has_two_level && !single_level_chunks.empty())
        return Status::Ready;

    bool pushed_to_output = false;

    /// Output if has data.
    if (has_two_level)
        pushed_to_output = tryPushTwoLevelData();

    /// Read next bucket if can.
    for (; ; ++current_bucket)
    {
        bool finished = true;
        bool need_data = false;

        auto in = inputs.begin();
        for (size_t input_num = 0; input_num < num_inputs; ++input_num, ++in)
        {
            if (in->isFinished())
                continue;

            finished = false;

            if (!need_input(input_num))
                continue;

            in->setNeeded();

            if (!in->hasData())
            {
                wait_input_ports_numbers.insert(input_num);
                need_data = true;
                continue;
            }

            auto chunk = in->pull();
            addChunk(std::move(chunk), input_num);

            if (!in->isFinished() && need_input(input_num))
            {
                wait_input_ports_numbers.insert(input_num);
                need_data = true;
            }
        }

        if (has_two_level && !single_level_chunks.empty())
            return Status::Ready;

        if (finished)
        {
            all_inputs_finished = true;
            break;
        }

        if (need_data)
            return Status::NeedData;
    }

    if (pushed_to_output)
        return Status::PortFull;

    if (has_two_level)
    {
        if (tryPushTwoLevelData())
            return Status::PortFull;

        /// Sanity check. If new bucket was read, we should be able to push it.
        /// This is always false, but we still keep this condition in case the code will be changed.
        if (!all_inputs_finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "GroupingAggregatedTransform has read new two-level bucket, but couldn't push it.");
    }
    else
    {
        if (!all_inputs_finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "GroupingAggregatedTransform should have read all chunks for single level aggregation, "
                            "but not all of the inputs are finished.");

        if (tryPushSingleLevelData())
            return Status::PortFull;
    }

    /// If we haven't pushed to output, then all data was read. Push overflows if have.
    if (tryPushOverflowData())
        return Status::PortFull;

    output.finish();
    return Status::Finished;
}

void GroupingAggregatedTransform::addChunk(Chunk chunk, size_t input)
{
    if (!chunk.hasRows())
        return;

    if (chunk.getChunkInfos().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk in GroupingAggregatedTransform.");

    if (auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>())
    {
        Int32 bucket = agg_info->bucket_num;
        bool is_overflows = agg_info->is_overflows;

        if (is_overflows)
            overflow_chunks.emplace_back(std::move(chunk));
        else if (bucket < 0)
            single_level_chunks.emplace_back(std::move(chunk));
        else
        {
            chunks_map[bucket].emplace_back(std::move(chunk));
            has_two_level = true;
            last_bucket_number[input] = bucket;
        }
    }
    else if (chunk.getChunkInfos().get<ChunkInfoWithAllocatedBytes>())
    {
        single_level_chunks.emplace_back(std::move(chunk));
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Chunk should have AggregatedChunkInfo/ChunkInfoWithAllocatedBytes in GroupingAggregatedTransform.");
    }
}

void GroupingAggregatedTransform::work()
{
    /// Convert single level data to two level.
    if (!single_level_chunks.empty())
    {
        const auto & header = getInputs().front().getHeader();  /// Take header from input port. Output header is empty.
        auto block = header.cloneWithColumns(single_level_chunks.back().detachColumns());
        single_level_chunks.pop_back();
        auto blocks = params->aggregator.convertBlockToTwoLevel(block);

        for (auto & cur_block : blocks)
        {
            if (!cur_block)
                continue;

            Int32 bucket = cur_block.info.bucket_num;
            auto chunk_info = std::make_shared<AggregatedChunkInfo>();
            chunk_info->bucket_num = bucket;

            auto chunk = Chunk(cur_block.getColumns(), cur_block.rows());
            chunk.getChunkInfos().add(std::move(chunk_info));

            chunks_map[bucket].emplace_back(std::move(chunk));
        }
    }
}


MergingAggregatedBucketTransform::MergingAggregatedBucketTransform(
    AggregatingTransformParamsPtr params_, const SortDescription & required_sort_description_)
    : ISimpleTransform({}, params_->getHeader(), false), params(std::move(params_)), required_sort_description(required_sort_description_)
{
    setInputNotNeededAfterRead(true);
}

void MergingAggregatedBucketTransform::transform(Chunk & chunk)
{
    auto chunks_to_merge = chunk.getChunkInfos().get<ChunksToMerge>();
    if (!chunks_to_merge)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergingAggregatedSimpleTransform chunk must have ChunkInfo with type ChunksToMerge.");

    auto header = params->aggregator.getHeader(false);

    BlocksList blocks_list;
    for (auto & cur_chunk : *chunks_to_merge->chunks)
    {
        if (cur_chunk.getChunkInfos().empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk in MergingAggregatedBucketTransform.");

        if (auto agg_info = cur_chunk.getChunkInfos().get<AggregatedChunkInfo>())
        {
            Block block = header.cloneWithColumns(cur_chunk.detachColumns());
            block.info.is_overflows = agg_info->is_overflows;
            block.info.bucket_num = agg_info->bucket_num;

            blocks_list.emplace_back(std::move(block));
        }
        else if (cur_chunk.getChunkInfos().get<ChunkInfoWithAllocatedBytes>())
        {
            Block block = header.cloneWithColumns(cur_chunk.detachColumns());
            block.info.is_overflows = false;
            block.info.bucket_num = -1;

            blocks_list.emplace_back(std::move(block));
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Chunk should have AggregatedChunkInfo/ChunkInfoWithAllocatedBytes in MergingAggregatedBucketTransform.");
        }
    }

    auto res_info = std::make_shared<AggregatedChunkInfo>();
    res_info->is_overflows = chunks_to_merge->is_overflows;
    res_info->bucket_num = chunks_to_merge->bucket_num;
    res_info->chunk_num = chunks_to_merge->chunk_num;
    chunk.getChunkInfos().add(std::move(res_info));

    auto block = params->aggregator.mergeBlocks(blocks_list, params->final, is_cancelled);

    if (!required_sort_description.empty())
        sortBlock(block, required_sort_description);

    size_t num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}


SortingAggregatedTransform::SortingAggregatedTransform(size_t num_inputs_, AggregatingTransformParamsPtr params_)
    : IProcessor(InputPorts(num_inputs_, params_->getHeader()), {params_->getHeader()})
    , num_inputs(num_inputs_)
    , params(std::move(params_))
    , last_bucket_number(num_inputs, std::numeric_limits<Int32>::min())
    , is_input_finished(num_inputs, false)
{
}

bool SortingAggregatedTransform::tryPushChunk()
{
    auto & output = outputs.front();

    if (chunks.empty())
        return false;

    /// Chunk with min current bucket.
    auto it = chunks.begin();
    auto cur_bucket = it->first;

    /// Check that can push it
    for (size_t input = 0; input < num_inputs; ++input)
        if (!is_input_finished[input] && last_bucket_number[input] < cur_bucket)
            return false;

    output.push(std::move(it->second));
    chunks.erase(it);
    return true;
}

void SortingAggregatedTransform::addChunk(Chunk chunk, size_t from_input)
{
    auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
    if (!agg_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Chunk should have AggregatedChunkInfo in SortingAggregatedTransform.");

    Int32 bucket = agg_info->bucket_num;
    bool is_overflows = agg_info->is_overflows;

    if (is_overflows)
        overflow_chunk = std::move(chunk);
    else
    {
        if (chunks[bucket])
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "SortingAggregatedTransform already got bucket with number {}", bucket);
        }

        chunks[bucket] = std::move(chunk);
        last_bucket_number[from_input] = bucket;
    }
}

IProcessor::Status SortingAggregatedTransform::prepare()
{
    /// Check can output.
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        chunks.clear();
        last_bucket_number.clear();
        return Status::Finished;
    }

    /// Check can push (to avoid data caching).
    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();

        return Status::PortFull;
    }

    /// Push if have min version.
    bool pushed_to_output = tryPushChunk();

    bool need_data = false;
    bool all_finished = true;

    /// Try read anything.
    auto in = inputs.begin();
    for (size_t input_num = 0; input_num < num_inputs; ++input_num, ++in)
    {
        if (in->isFinished())
        {
            is_input_finished[input_num] = true;
            continue;
        }

        /// We want to keep not more than `num_inputs` buckets in memory (and there will be only a single chunk with the given `bucket_id`).
        const bool bucket_from_this_input_still_in_memory = chunks.contains(last_bucket_number[input_num]);
        if (bucket_from_this_input_still_in_memory)
        {
            all_finished = false;
            continue;
        }

        in->setNeeded();

        if (!in->hasData())
        {
            need_data = true;
            all_finished = false;
            continue;
        }

        auto chunk = in->pull();
        addChunk(std::move(chunk), input_num);

        if (in->isFinished())
        {
            is_input_finished[input_num] = true;
        }
        else
        {
            /// If chunk was pulled, then we need data from this port.
            need_data = true;
            all_finished = false;
        }
    }

    if (pushed_to_output)
        return Status::PortFull;

    if (tryPushChunk())
        return Status::PortFull;

    if (need_data)
        return Status::NeedData;

    if (!all_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SortingAggregatedTransform has read bucket, but couldn't push it.");

    if (overflow_chunk)
    {
        output.push(std::move(overflow_chunk));
        return Status::PortFull;
    }

    output.finish();
    return Status::Finished;
}


void addMergingAggregatedMemoryEfficientTransform(
    Pipe & pipe,
    AggregatingTransformParamsPtr params,
    size_t num_merging_processors)
{
    pipe.addTransform(std::make_shared<GroupingAggregatedTransform>(pipe.getHeader(), pipe.numOutputPorts(), params));

    if (num_merging_processors <= 1)
    {
        /// --> GroupingAggregated --> MergingAggregatedBucket -->
        pipe.addTransform(std::make_shared<MergingAggregatedBucketTransform>(params));
        return;
    }

    /// -->                                        --> MergingAggregatedBucket -->
    /// --> GroupingAggregated --> ResizeProcessor --> MergingAggregatedBucket --> SortingAggregated -->
    /// -->                                        --> MergingAggregatedBucket -->

    pipe.resize(num_merging_processors);

    pipe.addSimpleTransform([params](const Block &)
    {
        return std::make_shared<MergingAggregatedBucketTransform>(params);
    });

    pipe.addTransform(std::make_shared<SortingAggregatedTransform>(num_merging_processors, params));
}

}
