#pragma once

#include <Core/Types.h>
#include <Storages/MutationCommands.h>


namespace DB
{

/// A mutation entry for non-replicated MergeTree storage engines.
struct MergeTreeMutationEntry
{
    time_t create_time = 0;

    Int64 block_number;
    MutationCommands commands;
};

}
