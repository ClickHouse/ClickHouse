#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Interpreters/Context.h>
#include "Interpreters/AggregatingMemoryHolder.h"

namespace DB
{

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
                                const SortDescription & group_by_description,
                                size_t max_block_size_, size_t max_block_bytes_,
                                ManyAggregatedDataPtr many_data, size_t current_variant);

    AggregatingInOrderTransform(Block header, AggregatingTransformParamsPtr params,
                                const SortDescription & group_by_description,
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
    SortDescriptionWithPositions group_by_description;

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
    FinalizeAggregatedTransform(Block header, AggregatingTransformParamsPtr params_)
        : ISimpleTransform({std::move(header)}, {params_->getHeader()}, true)
        , params(params_) {}

    void transform(Chunk & chunk) override
    {
        if (params->final)
            finalizeChunk(chunk);
        else if (!chunk.getChunkInfo())
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            chunk.setChunkInfo(std::move(info));
        }
    }

    String getName() const override { return "FinalizeAggregatedTransform"; }

private:
    AggregatingTransformParamsPtr params;
};

class AppendFinalizedTransform : public ISimpleTransform
{
public:
    AppendFinalizedTransform(Block input_header, Block output_header)
        : ISimpleTransform({std::move(input_header)}, {std::move(output_header)}, true) {}

    void transform(Chunk & chunk) override
    {
        for (const auto& column: chunk.getColumns()) {
            if (typeid_cast<const ColumnAggregateFunction *>(column.get())) {
                auto mut_column = IColumn::mutate(column);
                chunk.addColumn(ColumnAggregateFunction::convertToValues(IColumn::mutate(std::move(mut_column))));
            }
        }
    }

    String getName() const override { return "AppendFinalizedTransform"; }
};

class LookupingTransform : public ISimpleTransform
{
public:
    LookupingTransform(Block input_header, Block output_header, AggregatingTransformParamsPtr params_, AggregatingMemoryHolder holder_)
        : ISimpleTransform({std::move(input_header)}, {std::move(output_header)}, true)
        , params(params_)
        , holder(holder_) {}

    void transform(Chunk & chunk) override
    {
        Columns columns = chunk.getColumns();
        ColumnRawPtrs key_columns(params->params.keys_size);
        for (size_t i = 0; i < params->params.keys_size; ++i) {
            key_columns[i] = columns.at(params->params.keys[i]).get();
        }
        Block res = holder.lookupBlock(key_columns);
        chunk.setColumns(res.getColumns(), chunk.getNumRows());
    }

    String getName() const override { return "LookupingTransform"; }

private:
    AggregatingTransformParamsPtr params;

    AggregatingMemoryHolder holder;
};

}
