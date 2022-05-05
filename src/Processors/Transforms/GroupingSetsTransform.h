#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

/*
 * This transform is used as the last step of aggregation with GROUPING SETS modifier.
 * The main purpose is to add a '__grouping_set' column which stores information
 * about grouping keys set used to generate rows.
 * '__grouping_set' column is required during MergeAggregated step to distinguish
 * generated on different shards rows.
*/
class GroupingSetsTransform : public ISimpleTransform
{
public:
    GroupingSetsTransform(
        Block input_header,
        Block output_header,
        AggregatingTransformParamsPtr params,
        ColumnNumbers const & missing_columns,
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
