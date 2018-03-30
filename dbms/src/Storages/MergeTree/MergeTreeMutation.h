#pragma once

#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <common/logger_useful.h>


namespace DB
{

class MergeTreeMutation
{
public:
    MergeTreeMutation(MergeTreeData & data_, Int64 version_, std::vector<MutationCommand> commands_);

    MergeTreeData::MutableDataPartPtr executeOnPart(const MergeTreeData::DataPartPtr & part, const Context & context) const;

    /// TODO: private
    MergeTreeData & data;
    const Int64 version;
    std::vector<MutationCommand> commands;
    Logger * log;
};

}
