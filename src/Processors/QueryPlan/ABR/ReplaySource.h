#pragma once

#include <optional>

#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Processors/ISource.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Source that deserializes blocks from a ReadBuffer written by NativeBufferingFormat.
class ReplaySource : public ISource
{
public:
    /// The values of `rows_before_limit_at_least` and `rows_before_aggregation` of the pipeline that wrote the blocks.
    struct RowsBeforeSteps
    {
        std::optional<size_t> limit;
        std::optional<size_t> aggregation;
    };

    ReplaySource(
        SharedHeader header,
        CascadeWriteBuffer::WriteBufferPtrs owners_,
        std::unique_ptr<ReadBuffer> buf_,  /// `buf_` is null if the pipeline wrote no blocks.
        bool add_aggregation_info_,
        RowsBeforeSteps rows_before_steps_)
        : ISource(header, false) /// No auto_progress because the rows were already reported by the attempt's pipeline
        , buffer_owners(std::move(owners_))
        , buffer(std::move(buf_))
        , add_aggregation_info(add_aggregation_info_)
        , rows_before_steps(rows_before_steps_)
    {
        if (buffer)
            reader.emplace(*buffer, *header, DBMS_TCP_PROTOCOL_VERSION);
    }

    String getName() const override { return "ReplaySource"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_counter.swap(counter); }
    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation_counter.swap(counter); }

protected:
    Chunk generate() override
    {
        Block block;
        if (reader)
            block = reader->read();

        if (block.empty())
        {
            addRowsBeforeSteps();
            return {};
        }

        replayed_rows += block.rows();
        Chunk chunk(block.getColumns(), block.rows());
        if (add_aggregation_info)
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;
            info->out_of_order_buckets = block.info.out_of_order_buckets;
            chunk.getChunkInfos().add(std::move(info));
        }
        return chunk;
    }

private:
    /// The values of the pipeline that wrote the blocks are added to the counters of this pipeline. If that pipeline applied no limit, but
    /// this one has a limit above this source, which then does not count its input, the rows read from this source are counted instead.
    void addRowsBeforeSteps()
    {
        if (rows_before_limit_counter)
        {
            if (rows_before_steps.limit)
                rows_before_limit_counter->add(*rows_before_steps.limit);
            else if (rows_before_limit_counter->hasAppliedStep())
                rows_before_limit_counter->add(replayed_rows);
        }

        if (rows_before_aggregation_counter && rows_before_steps.aggregation)
            rows_before_aggregation_counter->add(*rows_before_steps.aggregation);
    }

    /// Owns the spill-files that `buffer` reads from, so it is declared before `buffer` to outlive it.
    CascadeWriteBuffer::WriteBufferPtrs buffer_owners;
    std::unique_ptr<ReadBuffer> buffer;
    std::optional<NativeReader> reader;
    bool add_aggregation_info;

    RowsBeforeSteps rows_before_steps;
    RowsBeforeStepCounterPtr rows_before_limit_counter;
    RowsBeforeStepCounterPtr rows_before_aggregation_counter;
    size_t replayed_rows = 0;
};

}
