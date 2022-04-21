#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class GroupingSetsTransform : public ISimpleTransform
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
    void transform(Chunk & chunk) override;

private:
    AggregatingTransformParamsPtr params;
    const ColumnNumbers missing_columns;
    const size_t set_id;
    const size_t output_size;
};

}
