#pragma once

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

#include <deque>
#include <memory>
#include <optional>
#include <unordered_map>

namespace DB
{

class IMergeTreeDataPart;
class MergeTreeData;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// The only purpose of this struct is that serialize and deserialize methods
/// they look natural here because we can fully serialize and then deserialize original DataPart class.
struct RangesInDataPartDescription
{
    MergeTreePartInfo info{};
    MarkRanges ranges{};
    size_t rows = 0;
    String projection_name;

    /// Initiator-provided hint for task sizing on replicas. Technically, all replicas send this value with the
    /// initial announcement request, but we always use the value from the replica local to the coordinator.
    /// The initiator computes this per part after PK analysis and propagates back to replicas in read request responses.
    size_t min_marks_per_task = 0;

    /// Total mark count of the underlying part on disk (NOT of the analyzed `ranges` above).
    /// Populated from `data_part->index_granularity->getMarksCountWithoutFinal` in
    /// `RangesInDataPart::getDescription`. Used by `ParallelReplicasReadingCoordinator` as a cheap
    /// sanity check (mark count mismatch implies divergent underlying parts), but mark count alone
    /// is not a part identity. A value of `0` means the field was not populated (older replica
    /// protocol or coordinator-internal queue entry).
    size_t total_marks_in_part = 0;

    /// Content fingerprint of the underlying part: the two halves of
    /// `data_part->checksums.getTotalChecksumUInt128`. Two replicas that hold the SAME on-disk
    /// part must produce the same fingerprint (the checksum is computed over the part's file
    /// contents and is independent of per-replica PK or skip-index analysis). Two replicas that
    /// hold genuinely different parts that happen to share a name (for example, two non-replicated
    /// `MergeTree` instances each created from independent local inserts that produced parts
    /// named `all_1_1_0`) will produce different fingerprints, even when their `total_marks_in_part`
    /// happen to coincide. The coordinator uses the fingerprint to reject the latter case while
    /// still accepting the former. A value of `(0, 0)` means the field was not populated (older
    /// replica protocol, coordinator-internal queue entry, or a part whose checksums were not
    /// loaded); the coordinator skips fingerprint validation and falls back to `total_marks_in_part`
    /// in that case.
    UInt64 part_checksum_low64 = 0;
    UInt64 part_checksum_high64 = 0;

    /// Whether the announcing replica's table guarantees that a part name identifies the same
    /// content on every cluster member.
    enum class PartNameIdentity : UInt8
    {
        /// Field was not populated: the announcement came from a replica whose protocol predates
        /// `DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PART_FINGERPRINT`, or the description is a
        /// coordinator-internal queue entry.
        Unknown = 0,
        /// Part names are allocated per node and the data is node-local, as in a plain `MergeTree`
        /// on ordinary local or per-node remote disks: block numbers come from a node-local
        /// `SimpleIncrement`, so two cluster members can independently produce same-named parts
        /// with divergent content. Same-named parts MUST be verified by content fingerprint; the
        /// coordinator fails closed when the fingerprint is unavailable.
        NodeLocal = 1,
        /// A part name implies identical content on every cluster member, so same-named parts are
        /// safe to merge even without a fingerprint. Two independent guarantees land here:
        /// `ReplicatedMergeTree` and descendants (block numbers come from a Keeper-coordinated
        /// counter), and a plain `MergeTree` whose data lives on shared-metadata storage
        /// (`MetadataStorageType::Plain`, `PlainRewritable`, `StaticWeb`, `WebIndex`, `Keeper`),
        /// where every cluster member reads literally the same set of parts.
        ClusterWide = 2,
    };

    /// Populated by `RangesInDataPart::getDescription` from the table's replication support and the
    /// metadata type of its disks. Used by `ParallelReplicasReadingCoordinator` to decide whether a
    /// missing part fingerprint is tolerable (`ClusterWide`: yes, guaranteed by the engine or by
    /// shared storage) or must fail closed (`NodeLocal`: same-named parts may hold divergent data).
    PartNameIdentity part_name_identity = PartNameIdentity::Unknown;

    void serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const;
    String describe() const;
    void deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version);
    String getPartOrProjectionName() const;
};

/// Whether a part name of `storage` identifies the same content on every cluster member: the engine
/// coordinates block numbers through Keeper (`ReplicatedMergeTree` and descendants), or all of the
/// table's data lives on shared-metadata storage where every member enumerates the same parts.
/// Otherwise part names are node-local and same-named parts may hold divergent content.
///
/// Deriving this inspects the table's storage policy, which takes a global lock, so callers that
/// describe many parts of the same table should derive it once and pass it down as a hint.
RangesInDataPartDescription::PartNameIdentity partNameIdentityOf(const MergeTreeData & storage);

struct RangesInDataPartsDescription: public std::deque<RangesInDataPartDescription>
{
    using std::deque<RangesInDataPartDescription>::deque;

    void serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const;
    String describe() const;
    void deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version);

    void merge(const RangesInDataPartsDescription & other);
};

struct PartOffsetRange
{
    size_t begin;
    size_t end;
};

struct PartOffsetRanges : public std::vector<PartOffsetRange>
{
    /// Tracks the total number of rows to determine if using the projection index is worthwhile.
    size_t total_rows = 0;

    /// Used to determine whether offsets can fit in 32-bit or require 64-bit.
    size_t max_part_offset = 0;

    /// Returns true if the ranges collectively cover the full range [0, total_rows)
    bool isContiguousFullRange() const { return total_rows == max_part_offset + 1; }

    /// Checks if the given offset falls within any of the stored ranges.
    /// Each range is treated as a half-open interval: [begin, end)
    bool contains(UInt64 offset) const
    {
        if (empty())
            return false;

        /// Binary search for the first range whose 'begin' is greater than offset.
        auto it = std::upper_bound(begin(), end(), offset, [](UInt64 value, const auto & range) { return value < range.begin; });

        if (it == begin())
            return false;

        /// The range before 'it' might contain the offset.
        --it;
        return offset < it->end;
    }
};

struct IMergeTreeIndexGranule;
using MergeTreeIndexGranulePtr = std::shared_ptr<IMergeTreeIndexGranule>;
using IndexGranulesMap = std::unordered_map<String, MergeTreeIndexGranulePtr>;

/// A vehicle which transports additional information to optimize searches
struct RangesInDataPartReadHints
{
    /// Currently only information related to vector search
    std::optional<NearestNeighbours> vector_search_results;
    /// If false, `vector_search_results` may still be used for mark pruning or
    /// to fill `_distance`, but the reader must not filter individual rows by
    /// these offsets. If true, the reader also keeps only the candidate rows
    /// from these offsets inside the selected mark ranges.
    bool use_vector_search_result_filter = false;
    /// Pre-computed index granules for indexes that are
    /// created for the whole part. For example, text indexes.
    IndexGranulesMap index_granules;
};

struct RangesInDataPart
{
    DataPartPtr data_part;
    DataPartPtr parent_part;
    size_t part_index_in_query;
    size_t part_starting_offset_in_query;
    MarkRanges ranges;
    MarkRanges exact_ranges;
    RangesInDataPartReadHints read_hints;

    /// The above "ranges" member is the selected ranges after index analysis.
    /// Index analysis has 2 steps : 1) Filter by primary key   2) Filter by skip indexes
    /// Below member saves a snapshot of the selected ranges after primary key analysis (optional),
    /// currently done only for use_skip_indexes_if_final_exact_mode=1
    std::optional<MarkRanges> ranges_snapshot_after_pk_analysis;

    /// Offset ranges from parent part, used during projection index reading.
    PartOffsetRanges parent_ranges;

    RangesInDataPart(
        const DataPartPtr & data_part_,
        const DataPartPtr & parent_part_,
        size_t part_index_in_query_,
        size_t part_starting_offset_in_query_,
        const MarkRanges & ranges_,
        const RangesInDataPartReadHints & read_hints_);

    explicit RangesInDataPart(
        const DataPartPtr & data_part_,
        const DataPartPtr & parent_part_ = nullptr,
        size_t part_index_in_query_ = 0,
        size_t part_starting_offset_in_query_ = 0);

    /// `part_name_identity_hint` lets a caller that describes many parts of the same table compute
    /// the identity class once instead of per part - deriving it inspects the table's storage policy,
    /// which takes a global lock. When not given, it is derived here.
    RangesInDataPartDescription getDescription(
        std::optional<RangesInDataPartDescription::PartNameIdentity> part_name_identity_hint = {}) const;

    size_t getMarksCount() const;
    size_t getRowsCount() const;
};

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

struct RangesInDataParts : public std::vector<RangesInDataPart>
{
    using std::vector<RangesInDataPart>::vector; /// NOLINT(modernize-type-traits)

    explicit RangesInDataParts(const DataPartsVector & parts);
    RangesInDataPartsDescription getDescriptions() const;

    size_t getMarksCountAllParts() const;
    size_t getRowsCountAllParts() const;
};
using RangesInDataPartsPtr = std::shared_ptr<const RangesInDataParts>;

}
