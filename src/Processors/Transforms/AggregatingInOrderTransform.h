#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>

namespace DB
{

class AggregatingInOrderTransform : public IProcessor
{

public:
    AggregatingInOrderTransform(Block header, AggregatingTransformParamsPtr params,
                                const SortDescription & group_by_description, size_t res_block_size,
                                ManyAggregatedDataPtr many_data, size_t current_variant);

    AggregatingInOrderTransform(Block header, AggregatingTransformParamsPtr params,
                                const SortDescription & group_by_description, size_t res_block_size);

    ~AggregatingInOrderTransform() override;

    String getName() const override { return "AggregatingInOrderTransform"; }

    Status prepare() override;

    void work() override;

    void consume(Chunk chunk);

private:
    void generate();

    size_t res_block_size;
    size_t cur_block_size = 0;

    MutableColumns res_key_columns;
    MutableColumns res_aggregate_columns;

    AggregatingTransformParamsPtr params;

    SortDescription group_by_description;

    Aggregator::AggregateColumns aggregate_columns;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;

    bool need_generate = false;
    bool block_end_reached = false;
    bool is_consume_finished = false;

    Block res_header;
    Chunk current_chunk;
    Chunk to_push_chunk;

    Logger * log = &Logger::get("AggregatingInOrderTransform");
};


class FinalizingSimpleTransform : public ISimpleTransform
{
public:
    FinalizingSimpleTransform(Block header, AggregatingTransformParamsPtr params)
        : ISimpleTransform({std::move(header)}, {params->getHeader(true)}, true) {}

    void transform(Chunk & chunk) override
    {
        finalizeChunk(chunk);
    }

    String getName() const override { return "FinalizingSimpleTransform"; }
};


}
