#pragma once

#include <Core/Types.h>
#include <map>


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

}
