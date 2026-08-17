#pragma once

#include <base/types.h>
#include <Core/Names.h>
#include <optional>
#include <map>
#include <ctime>


namespace DB
{

/// Postpone reasons for parts that cannot be merged or mutated
namespace PostponeReasons
{
    inline constexpr auto QUORUM_NOT_REACHED = "Quorum not reached yet";
    inline constexpr auto REACH_MEMORY_LIMIT = "Reach memory limit";
    inline constexpr auto EXCEED_MAX_QUEUED_MERGES = "Exceed max queued merges";
    inline constexpr auto NO_FREE_THREADS = "No free threads in pool";
    inline constexpr auto EXCEED_MAX_PART_SIZE = "Exceed max source part size";
    inline constexpr auto HIT_MUTATION_BACKOFF = "Hit mutation backoff policy";
    inline constexpr auto VERSION_NOT_VISIBLE = "Not visible by transaction version";

    /// Special key in parts_postpone_reasons map indicating the reason applies to all parts
    inline constexpr auto ALL_PARTS_KEY = "all_parts";
}

struct MergeTreeMutationStatus
{
/// NOLINTBEGIN(readability-redundant-string-init)
    String id = "";
    String command = "";
    time_t create_time = 0;
    std::map<String, Int64> block_numbers{};

    /// Parts that are currently being mutated.
    Names parts_in_progress_names = {};

    /// Parts that should be mutated/merged or otherwise moved to Obsolete state for this mutation to complete.
    Names parts_to_do_names = {};

    /// Map of part names to reasons why they are postponed
    std::map<String, String> parts_postpone_reasons = {};

    /// If the mutation is done. Note that in case of ReplicatedMergeTree parts_to_do == 0 doesn't imply is_done == true.
    bool is_done = false;

    String latest_failed_part = "";
    time_t latest_fail_time = 0;
    String latest_fail_reason = "";
    String latest_fail_error_code_name = "";

    /// FIXME: currently unused, but would be much better to report killed mutations with this flag.
    bool is_killed = false;

    /// The on-disk bytes of `parts_to_do_names`. Derived on read, like `parts_to_do`.
    UInt64 bytes_to_do = 0;
    /// Estimated finished fraction, from 0 to 1, byte-weighted against the parts to rewrite.
    /// Unset while the remaining work is not known yet: see StorageReplicatedMergeTree.
    std::optional<Float64> progress = {};
/// NOLINTEND(readability-redundant-string-init)
};

/// Active parts as (min_block, bytes on disk), used to size the scope of a mutation.
/// One per partition on the replicated path, where block numbers are allocated per partition.
using PartBlockBytes = std::vector<std::pair<Int64, UInt64>>;

/// Sort by `min_block` and replace each byte count with the running total, so that
/// `getBytesBeforeBlock` can binary-search the result.
void accumulatePartBlockBytes(PartBlockBytes & parts);

/// On-disk bytes of the parts that predate `block_number`, i.e. the ones that mutation must rewrite,
/// done or not. Parts inserted later have a higher `min_block` and were never in its scope.
UInt64 getBytesBeforeBlock(const PartBlockBytes & parts, Int64 block_number);

/// Check mutation status and throw exception in case of error during mutation
/// (latest_fail_reason not empty) or if mutation was killed (status empty
/// optional). mutation_ids passed separately, because status may be empty and
/// we can execute multiple mutations at once
void checkMutationStatus(std::optional<MergeTreeMutationStatus> & status, const std::set<String> & mutation_ids);

}
