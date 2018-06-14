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
};

}
