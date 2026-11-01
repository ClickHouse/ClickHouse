#pragma once

#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>


namespace DB
{

/** Returns the rows in a uniformly random order, as a single stream sorted by the key column.
  * Used for the `SHUFFLE` clause (with an internal key column) and for `ORDER BY rand()` (with the `rand()` column as the key).
  *
  * Every stream is shuffled by `PartialShuffleTransform` in O(n), which fills the key column with random keys
  * in sorted order, and the streams are merged by the key column. See `PartialShuffleTransform` for details.
  * The output can be merged with other streams processed this way (e.g. on remote servers) by `MergingSortedTransform`.
  *
  * If `limit` is not zero, only at least the first `limit` rows of the result are guaranteed to be correct,
  * as for `SortingStep`, and the memory usage is O(limit).
  */
class ShuffleStep : public ITransformingStep
{
public:
    /// The name of the internal key column for the `SHUFFLE` clause.
    static constexpr auto internal_key_column_name = "__shuffle_key";

    /// `direction` is 1 if the output should be sorted by the key column ascending, -1 - descending.
    ShuffleStep(
        const SharedHeader & input_header,
        String key_column_name_,
        int direction_,
        UInt64 limit_,
        size_t max_block_size_);

    String getName() const override { return "Shuffle"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const String & getKeyColumnName() const { return key_column_name; }
    UInt64 getLimit() const { return limit; }

    QueryPlanStepPtr clone() const override;
    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    SortDescription getKeySortDescription() const;

    const String key_column_name;
    const int direction;
    const UInt64 limit;
    const size_t max_block_size;
};

}
