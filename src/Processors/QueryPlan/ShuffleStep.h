#pragma once

#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>


namespace DB
{

/** Returns the rows in a uniformly random order as a single stream, in O(n) time (see `ShuffleTransform.h`).
  * Used for the `SHUFFLE` clause and for `ORDER BY rand()`.
  *
  * Without a limit, the rows are scattered into `max_threads` parts at random, every part is shuffled, and the parts
  * are concatenated. With a limit, every stream is sampled by reservoir sampling and the samples are merged, so only
  * at least the first `limit` rows of the result are returned, as for `SortingStep`, using O(limit) memory.
  *
  * Optionally fills a key column with sorted random keys, so that the result is sorted by it:
  * - for `ORDER BY rand()`, the existing `rand()` column is replaced;
  * - for `SHUFFLE` processed on a shard of a distributed query, an internal key column is appended, and the initiator
  *   merges the streams of the shards by it.
  */
class ShuffleStep : public ITransformingStep
{
public:
    /// The name of the internal key column for the `SHUFFLE` clause on shards.
    static constexpr auto internal_key_column_name = "__shuffle_key";

    struct Key
    {
        String column_name;
        /// The column exists in the input and its values are replaced; otherwise a `UInt64` column is appended.
        bool replace = false;
        /// 1 if the output should be sorted by the key column ascending, -1 - descending.
        int direction = 1;
    };

    ShuffleStep(
        const SharedHeader & input_header,
        std::optional<Key> key_,
        UInt64 limit_,
        size_t max_block_size_);

    [[nodiscard]] String getName() const override { return "Shuffle"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    [[nodiscard]] const std::optional<Key> & getKey() const { return key; }
    [[nodiscard]] UInt64 getLimit() const { return limit; }

    void serialize(Serialization & ctx) const override;
    [[nodiscard]] bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    [[nodiscard]] QueryPlanStepPtr clone() const override;
    [[nodiscard]] bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;

    const std::optional<Key> key;
    const UInt64 limit;
    const size_t max_block_size;
};

}
