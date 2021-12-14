#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

class GroupingSetsTransform : public IAccumulatingTransform
{
public:
    GroupingSetsTransform(Block header, AggregatingTransformParamsPtr params);
    String getName() const override { return "GroupingSetsTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    const ColumnNumbers & keys;
    const ColumnNumbersList & keys_vector;

    Chunks consumed_chunks;
    Chunk grouping_sets_chunk;
    Columns current_columns;
    std::unordered_map<size_t, ColumnPtr> current_zero_columns;

    UInt64 keys_vector_idx = 0;

    Poco::Logger * log = &Poco::Logger::get("GroupingSetsTransform");

    Chunk merge(Chunks && chunks, bool final);
};

}
