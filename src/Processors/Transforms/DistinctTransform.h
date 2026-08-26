#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// The streaming hash-based DISTINCT: emits the first occurrence of each key as soon as it is seen.
/// The deduplication logic itself lives in DistinctSetFilter (shared with ExternalDistinctTransform,
/// which additionally spills to disk under memory pressure).
class DistinctTransform final : public ISimpleTransform
{
public:
    /// max_bytes_before_pass_through_ (0 - disabled) is only for a preliminary DISTINCT followed by an
    /// exact one: when the memory usage of the query exceeds it, the transform frees its set and lets all
    /// rows through, leaving the deduplication to the final DISTINCT (which can spill to disk, see
    /// ExternalDistinctTransform).
    DistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        UInt64 max_bytes_before_pass_through_ = 0);

    String getName() const override { return "DistinctTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    DistinctSetFilter distinct_set;
    const UInt64 limit_hint;

    const UInt64 max_bytes_before_pass_through;
    bool pass_through = false;
};

}
