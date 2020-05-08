#pragma once

#include <Interpreters/Aggregator.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Core/SortDescription.h>

namespace DB
{

class AggregatingInOrderTransform : public IProcessor
{

public:
    AggregatingInOrderTransform(Block header, AggregatingTransformParamsPtr params, SortDescription & sort_description,
                                SortDescription & group_by_description, size_t max_block_size);

    ~AggregatingInOrderTransform() override;

    String getName() const override { return "AggregatingInOrderTransform"; }

    Status prepare() override;

    void work() override;

    void consume(Chunk chunk);

private:
    void generate();
//    size_t x = 1;
//    size_t sz = 0;

    size_t max_block_size;
    size_t res_block_size = 0;

    MutableColumns res_key_columns;
    MutableColumns res_aggregate_columns;

    AggregatingTransformParamsPtr params;

    SortDescription sort_description;
    SortDescription group_by_description;

    Aggregator::AggregateColumns aggregate_columns;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;

    bool need_generate = false;
    bool block_end_reached = false;
    bool is_consume_finished = false;

    Chunk current_chunk;
    Chunk to_push_chunk;

    Logger * log = &Logger::get("AggregatingInOrderTransform");
};

}
