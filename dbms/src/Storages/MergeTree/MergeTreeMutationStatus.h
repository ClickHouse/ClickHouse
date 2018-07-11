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

    /// A number of parts that should be mutated/merged or otherwise moved to Obsolete state for this mutation to complete.
    Int64 parts_to_do = 0;

    /// If the mutation is done. Note that in case of ReplicatedMergeTree parts_to_do == 0 doesn't imply is_done == true.
    bool is_done = false;
};

}
