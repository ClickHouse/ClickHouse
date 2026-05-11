#pragma once

#include <Storages/MergeTree/ANNIndex/IntervalSet.h>
#include <Core/Types.h>

#include <string>
#include <unordered_map>

namespace DB
{

class IANNGroupStorage;

/// Coverage information for a group: which `_block_number` ranges from which partitions are
/// included in this group's ANN index. The partition is identified by a 64-bit hash of its
/// `partition_id` (the hash algorithm and seed are recorded in `meta.json` by the builder).
///
/// `coverage.bin` on-disk layout:
///   header (16 bytes):
///       char[8] magic      = "ANNCOV__"
///       UInt32  version    = 1
///       UInt32  partition_count
///   body:
///       partition_count × (UInt64 partition_hash, IntervalSet<UInt64>)
///   footer (16 bytes):
///       UInt64 checksum    = XXH64 of body
///       char[8] magic_end  = "ENDCOV__"
class ANNGroupCoverage
{
public:
    /// Add `[min_block, max_block]` for `partition_hash`. Adjacent/overlapping ranges are merged.
    void addPart(UInt64 partition_hash, UInt64 min_block, UInt64 max_block);

    /// Check whether the entire `[min_block, max_block]` range for a given partition is covered
    /// by at least one of the stored intervals.
    bool containsPart(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const;

    bool empty() const { return by_partition_hash.empty(); }
    size_t partitionCount() const { return by_partition_hash.size(); }

    const std::unordered_map<UInt64, IntervalSet<UInt64>> & byPartitionHash() const
    {
        return by_partition_hash;
    }

    /// Serialise / deserialise through the single group-storage abstraction.
    void writeTo(IANNGroupStorage & storage) const;
    void readFrom(IANNGroupStorage & storage);

    /// Debug / logging helper: hex-dumps partition hashes together with interval summaries.
    std::string describe() const;

private:
    std::unordered_map<UInt64, IntervalSet<UInt64>> by_partition_hash;
};

}
