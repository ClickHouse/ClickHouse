#pragma once

#include <base/types.h>
#include <Core/Names.h>
#include <optional>
#include <map>
#include <ctime>


namespace DB
{


struct MergeTreeMutationStatus
{
    String id;
    String command;
    time_t create_time = 0;
    std::map<String, Int64> block_numbers;

    /// Parts that should be mutated/merged or otherwise moved to Obsolete state for this mutation to complete.
    Names parts_to_do_names;

    /// If the mutation is done. Note that in case of ReplicatedMergeTree parts_to_do == 0 doesn't imply is_done == true.
    bool is_done = false;

    String latest_failed_part;
    time_t latest_fail_time = 0;
    String latest_fail_reason;
};

/// Check mutation status and throw exception in case of error during mutation
/// (latest_fail_reason not empty) or if mutation was killed (status empty
/// optional). mutation_ids passed separately, because status may be empty and
/// we can execute multiple mutations at once
void checkMutationStatus(std::optional<MergeTreeMutationStatus> & status, const std::set<String> & mutation_ids);

}
