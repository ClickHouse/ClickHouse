#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Aggregator.h>
#include <Processors/ResizeProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct ChunksToMerge : public ChunkInfo
{
    std::unique_ptr<Chunks> chunks;
    Int32 bucket_num = -1;
    bool is_overflows = false;
};

GroupingAggregatedTransform::GroupingAggregatedTransform(
    const Block & header_, size_t num_inputs_, AggregatingTransformParamsPtr params_)
    : IProcessor(InputPorts(num_inputs_, header_), { Block() })
    , num_inputs(num_inputs_)
    , params(std::move(params_))
    , last_bucket_number(num_inputs, -1)
    , read_from_input(num_inputs, false)
{
}

void GroupingAggregatedTransform::readFromAllInputs()
{
    auto in = inputs.begin();
    read_from_all_inputs = true;

    for (size_t i = 0; i < num_inputs; ++i, ++in)
    {
        if (in->isFinished())
            continue;

        if (read_from_input[i])
            continue;

        in->setNeeded();

        if (!in->hasData())
        {
            read_from_all_inputs = false;
            continue;
        }

        auto chunk = in->pull();
        read_from_input[i] = true;
        addChunk(std::move(chunk), i);
    }
}

void GroupingAggregatedTransform::pushData(Chunks chunks, Int32 bucket, bool is_overflows)
{
    auto & output = outputs.front();

    auto info = std::make_shared<ChunksToMerge>();
    info->bucket_num = bucket;
    info->is_overflows = is_overflows;
    info->chunks = std::make_unique<Chunks>(std::move(chunks));

    Chunk chunk;
    chunk.setChunkInfo(std::move(info));
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

IProcessor::Status GroupingAggregatedTransform::prepare()
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

    /// Read first time from each input to understand if we have two-level aggregation.
    if (!read_from_all_inputs)
    {
        readFromAllInputs();
        if (!read_from_all_inputs)
            return Status::NeedData;
    }

    /// Convert single level to two levels if have two-level input.
    if (has_two_level && !single_level_chunks.empty())
        return Status::Ready;

    /// Check can push (to avoid data caching).
    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();

        return Status::PortFull;
    }

    bool pushed_to_output = false;

    /// Output if has data.
    if (has_two_level)
        pushed_to_output = tryPushTwoLevelData();

    auto need_input = [this](size_t input_num)
    {
        if (last_bucket_number[input_num] < current_bucket)
            return true;

        return expect_several_chunks_for_single_bucket_per_source && last_bucket_number[input_num] == current_bucket;
    };

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
                need_data = true;
                continue;
            }

            auto chunk = in->pull();
            addChunk(std::move(chunk), input_num);

            if (has_two_level && !single_level_chunks.empty())
                return Status::Ready;

            if (!in->isFinished() && need_input(input_num))
                need_data = true;
        }

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
        if (!all_inputs_finished)
            throw Exception("GroupingAggregatedTransform has read new two-level bucket, but couldn't push it.",
                            ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        if (!all_inputs_finished)
            throw Exception("GroupingAggregatedTransform should have read all chunks for single level aggregation, "
                            "but not all of the inputs are finished.", ErrorCodes::LOGICAL_ERROR);

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
    const auto & info = chunk.getChunkInfo();
    if (!info)
        throw Exception("Chunk info was not set for chunk in GroupingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

    const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
    if (!agg_info)
        throw Exception("Chunk should have AggregatedChunkInfo in GroupingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

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
            chunks_map[bucket].emplace_back(Chunk(cur_block.getColumns(), cur_block.rows(), std::move(chunk_info)));
        }
    }
}


MergingAggregatedBucketTransform::MergingAggregatedBucketTransform(AggregatingTransformParamsPtr params_)
    : ISimpleTransform({}, params_->getHeader(), false), params(std::move(params_))
{
    setInputNotNeededAfterRead(true);
}

void MergingAggregatedBucketTransform::transform(Chunk & chunk)
{
    const auto & info = chunk.getChunkInfo();
    const auto * chunks_to_merge = typeid_cast<const ChunksToMerge *>(info.get());

    if (!chunks_to_merge)
        throw Exception("MergingAggregatedSimpleTransform chunk must have ChunkInfo with type ChunksToMerge.",
                        ErrorCodes::LOGICAL_ERROR);

    auto header = params->aggregator.getHeader(false);

    BlocksList blocks_list;
    for (auto & cur_chunk : *chunks_to_merge->chunks)
    {
        const auto & cur_info = cur_chunk.getChunkInfo();
        if (!cur_info)
            throw Exception("Chunk info was not set for chunk in MergingAggregatedBucketTransform.",
                    ErrorCodes::LOGICAL_ERROR);

        const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(cur_info.get());
        if (!agg_info)
            throw Exception("Chunk should have AggregatedChunkInfo in MergingAggregatedBucketTransform.",
                    ErrorCodes::LOGICAL_ERROR);

        Block block = header.cloneWithColumns(cur_chunk.detachColumns());
        block.info.is_overflows = agg_info->is_overflows;
        block.info.bucket_num = agg_info->bucket_num;

        blocks_list.emplace_back(std::move(block));
    }

    auto res_info = std::make_shared<AggregatedChunkInfo>();
    res_info->is_overflows = chunks_to_merge->is_overflows;
    res_info->bucket_num = chunks_to_merge->bucket_num;
    chunk.setChunkInfo(std::move(res_info));

    auto block = params->aggregator.mergeBlocks(blocks_list, params->final);
    size_t num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}


SortingAggregatedTransform::SortingAggregatedTransform(size_t num_inputs_, AggregatingTransformParamsPtr params_)
    : IProcessor(InputPorts(num_inputs_, params_->getHeader()), {params_->getHeader()})
    , num_inputs(num_inputs_)
    , params(std::move(params_))
    , last_bucket_number(num_inputs, -1)
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
    const auto & info = chunk.getChunkInfo();
    if (!info)
        throw Exception("Chunk info was not set for chunk in SortingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

    const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
    if (!agg_info)
        throw Exception("Chunk should have AggregatedChunkInfo in SortingAggregatedTransform.", ErrorCodes::LOGICAL_ERROR);

    Int32 bucket = agg_info->bucket_num;
    bool is_overflows = agg_info->is_overflows;

    if (is_overflows)
        overflow_chunk = std::move(chunk);
    else
    {
        if (chunks[bucket])
            throw Exception("SortingAggregatedTransform already got bucket with number " + toString(bucket),
                    ErrorCodes::LOGICAL_ERROR);

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

        //all_finished = false;

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
        throw Exception("SortingAggregatedTransform has read bucket, but couldn't push it.",
                  ErrorCodes::LOGICAL_ERROR);

    if (overflow_chunk)
    {
        output.push(std::move(overflow_chunk));
        return Status::PortFull;
    }

    output.finish();
    return Status::Finished;
}


Processors createMergingAggregatedMemoryEfficientPipe(
        Block header,
        AggregatingTransformParamsPtr params,
        size_t num_inputs,
        size_t num_merging_processors)
{
    Processors processors;
    processors.reserve(num_merging_processors + 2);

    auto grouping = std::make_shared<GroupingAggregatedTransform>(header, num_inputs, params);
    processors.emplace_back(std::move(grouping));

    if (num_merging_processors <= 1)
    {
        /// --> GroupingAggregated --> MergingAggregatedBucket -->
        auto transform = std::make_shared<MergingAggregatedBucketTransform>(params);
        connect(processors.back()->getOutputs().front(), transform->getInputPort());

        processors.emplace_back(std::move(transform));
        return processors;
    }

    /// -->                                        --> MergingAggregatedBucket -->
    /// --> GroupingAggregated --> ResizeProcessor --> MergingAggregatedBucket --> SortingAggregated -->
    /// -->                                        --> MergingAggregatedBucket -->

    auto resize = std::make_shared<ResizeProcessor>(Block(), 1, num_merging_processors);
    connect(processors.back()->getOutputs().front(), resize->getInputs().front());
    processors.emplace_back(std::move(resize));

    auto sorting = std::make_shared<SortingAggregatedTransform>(num_merging_processors, params);
    auto out = processors.back()->getOutputs().begin();
    auto in = sorting->getInputs().begin();

    for (size_t i = 0; i < num_merging_processors; ++i, ++in, ++out)
    {
        auto transform = std::make_shared<MergingAggregatedBucketTransform>(params);
        transform->setStream(i);
        connect(*out, transform->getInputPort());
        connect(transform->getOutputPort(), *in);
        processors.emplace_back(std::move(transform));
    }

    processors.emplace_back(std::move(sorting));
    return processors;
}

}
