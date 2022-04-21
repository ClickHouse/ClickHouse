#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include "Core/ColumnNumbers.h"

namespace DB
{

class GroupingSetsTransform : public IAccumulatingTransform
{
public:
    GroupingSetsTransform(
        Block input_header,
        Block output_header,
        AggregatingTransformParamsPtr params,
        ColumnNumbersList const & missing_columns,
        size_t set_id);
    String getName() const override { return "GroupingSetsTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    const ColumnNumbers missing_columns;
    const size_t set_id;
    const size_t output_size;

    Chunks consumed_chunks;

    Poco::Logger * log = &Poco::Logger::get("GroupingSetsTransform");

    Chunk merge(Chunks && chunks, bool final);
};

}
