#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/finalizeChunk.h>

namespace DB
{

struct InputOrderInfo;
using InputOrderInfoPtr = std::shared_ptr<const InputOrderInfo>;

struct ChunkInfoWithAllocatedBytes : public ChunkInfo
{
    explicit ChunkInfoWithAllocatedBytes(Int64 allocated_bytes_)
        : allocated_bytes(allocated_bytes_) {}
    Int64 allocated_bytes;
};

class AggregatingInOrderTransform : public IProcessor
{
public:
    AggregatingInOrderTransform(Block header, AggregatingTransformParamsPtr params,
                                InputOrderInfoPtr group_by_info_,
                                const SortDescription & group_by_description_,
                                size_t max_block_size_, size_t max_block_bytes_,
                                ManyAggregatedDataPtr many_data, size_t current_variant);

    AggregatingInOrderTransform(Block header, AggregatingTransformParamsPtr params,
                                InputOrderInfoPtr group_by_info_,
                                const SortDescription & group_by_description_,
                                size_t max_block_size_, size_t max_block_bytes_);

    ~AggregatingInOrderTransform() override;

    String getName() const override { return "AggregatingInOrderTransform"; }

    Status prepare() override;

    void work() override;

    void consume(Chunk chunk);

private:
    void generate();
    void finalizeCurrentChunk(Chunk chunk, size_t key_end);

    size_t max_block_size;
    size_t max_block_bytes;
    size_t cur_block_size = 0;
    size_t cur_block_bytes = 0;

    MutableColumns res_key_columns;
    MutableColumns res_aggregate_columns;

    AggregatingTransformParamsPtr params;

    InputOrderInfoPtr group_by_info;
    /// For sortBlock()
    SortDescription sort_description;
    SortDescriptionWithPositions group_by_description;
    bool group_by_key = false;
    Block group_by_block;
    ColumnRawPtrs key_columns_raw;

    Aggregator::AggregateColumns aggregate_columns;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;
    UInt64 res_rows = 0;

    bool need_generate = false;
    bool block_end_reached = false;
    bool is_consume_started = false;
    bool is_consume_finished = false;

    Block res_header;
    Chunk current_chunk;
    Chunk to_push_chunk;

    Poco::Logger * log = &Poco::Logger::get("AggregatingInOrderTransform");
};


class FinalizeAggregatedTransform : public ISimpleTransform
{
public:
    FinalizeAggregatedTransform(Block header, AggregatingTransformParamsPtr params_);

    void transform(Chunk & chunk) override;
    String getName() const override { return "FinalizeAggregatedTransform"; }

private:
    AggregatingTransformParamsPtr params;
    ColumnsMask aggregates_mask;
};


}
