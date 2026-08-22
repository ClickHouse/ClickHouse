#pragma once

#include <base/types.h>
#include <Core/Names.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <optional>
#include <map>
#include <ctime>


namespace DB
{

/// The denominator for byte-weighted mutation progress: the byte weight of the mutation's remaining
/// scope, counted once per covered block range rather than once per part name.
struct MutationScopeInitialBytes
{
    UInt64 bytes = 0;
    std::map<MergeTreePartInfo, UInt64> counted_parts;

    /// Still-pending work reappearing under a new name (a merge of pending parts, or an earlier
    /// mutation's rewrite) replaces the weight of the counted parts it covers instead of adding to it.
    void account(const MergeTreePartInfo & info, UInt64 part_bytes);

    /// A finished mutation's scope cannot grow again, so only the scalar denominator is kept.
    void finalize();
};

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
    /// Time when the mutation was completed. Zero if the mutation is not done yet or if its
    /// completion time is unknown (see the `finish_time` column description in
    /// `StorageSystemMutations`). For replicated tables the value is per-replica.
    time_t finish_time = 0;
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

/// Check mutation status and throw exception in case of error during mutation
/// (latest_fail_reason not empty) or if mutation was killed (status empty
/// optional). mutation_ids passed separately, because status may be empty and
/// we can execute multiple mutations at once
void checkMutationStatus(std::optional<MergeTreeMutationStatus> & status, const std::set<String> & mutation_ids);

}
