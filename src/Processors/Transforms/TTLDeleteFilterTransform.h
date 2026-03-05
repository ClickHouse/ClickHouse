#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Storages/TTLDescription.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Interpreters/PreparedSets.h>
#include <Common/DateLUT.h>

namespace DB
{

/// Evaluates TTL delete expressions and produces a UInt8 filter column
/// without actually filtering the block. The filter column is then used
/// by the merging algorithm to set skip flags in row sources for vertical merge.
///
/// Also tracks new_ttl_info for each delete TTL entry and writes updated
/// TTL metadata to the data part when processing is complete, similar to
/// ITTLAlgorithm::finalize implementations.
class TTLDeleteFilterTransform : public ISimpleTransform
{
public:
    static inline const String TTL_FILTER_COLUMN_NAME = "_ttl_filter";

    TTLDeleteFilterTransform(
        const ContextPtr & context,
        const SharedHeader & header_,
        const StorageMetadataPtr & metadata_snapshot_,
        const IMergeTreeDataPart::TTLInfos & old_ttl_infos_,
        time_t current_time_,
        bool force_,
        const MergeTreeMutableDataPartPtr & data_part_);

    String getName() const override { return "TTLDeleteFilter"; }

    Status prepare() override;

    PreparedSets::Subqueries getSubqueries() { return std::move(subqueries_for_sets); }

    static SharedHeader transformHeader(const SharedHeader & header);

protected:
    void transform(Chunk & chunk) override;

private:
    struct DeleteTTLEntry
    {
        TTLExpressions expressions;
        TTLDescription description;
        IMergeTreeDataPart::TTLInfo old_ttl_info;
        IMergeTreeDataPart::TTLInfo new_ttl_info;
    };

    std::vector<DeleteTTLEntry> delete_ttl_entries;

    const time_t current_time;
    const bool force;
    const DateLUTImpl & date_lut;
    bool all_data_dropped = false;

    MergeTreeMutableDataPartPtr data_part;
    bool finalized = false;

    PreparedSets::Subqueries subqueries_for_sets;
    PaddedPODArray<Int64> timestamps;

    bool isTTLExpired(time_t ttl) const;
    bool isMinTTLExpired(const IMergeTreeDataPart::TTLInfo & info) const;

    void finalize();

    /// Convert a typed TTL column into a uniform Int64 timestamp array.
    /// Resolves the concrete column type once, then extracts all values in a tight loop.
    void extractTimestamps(const IColumn * ttl_column, size_t num_rows);
};

}
