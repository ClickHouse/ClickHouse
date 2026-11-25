#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Storages/TTLDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Interpreters/PreparedSets.h>
#include <Common/DateLUT.h>

namespace DB
{

/// Evaluates TTL delete expressions and attaches the resulting mask to the chunk, without
/// filtering the block or changing its header. The merging algorithm reads the mask to set skip
/// flags in row sources for a vertical merge.
class TTLDeleteFilterTransform final : public ISimpleTransform
{
public:
    /// Immutable state shared across all per-stream transform instances.
    /// Built once so that every instance references the same `FutureSet`
    /// objects that `CreatingSetStep` fills.
    struct SharedState
    {
        struct Entry
        {
            TTLExpressions expressions;
            TTLDescription description;
        };
        std::vector<Entry> entries;
        bool all_data_dropped = false;
        time_t current_time = 0;
    };

    /// Build the shared state once. Returns it together with the subqueries
    /// that must be passed to `CreatingSetStep`.
    static std::pair<std::shared_ptr<const SharedState>, PreparedSets::Subqueries> build(
        const ContextPtr & context,
        const StorageMetadataPtr & metadata_snapshot,
        const IMergeTreeDataPart::TTLInfos & old_ttl_infos,
        time_t current_time,
        bool force);

    TTLDeleteFilterTransform(const SharedHeader & header_, std::shared_ptr<const SharedState> shared_state_);

    String getName() const override { return "TTLDeleteFilter"; }

protected:
    void transform(Chunk & chunk) override;

private:
    std::shared_ptr<const SharedState> shared_state;
    const DateLUTImpl & date_lut;
    PaddedPODArray<Int64> timestamps;

    /// Convert a typed TTL column into a uniform Int64 timestamp array.
    /// Resolves the concrete column type once, then extracts all values in a tight loop.
    void extractTimestamps(const IColumn * ttl_column);
};

}
