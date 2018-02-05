#pragma once

#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <common/logger_useful.h>


namespace DB
{

class MergeTreeMutation
{
public:
    MergeTreeMutation(MergeTreeData & storage_, UInt32 version_, std::vector<MutationCommand> commands_);

    void execute(const Context & context);

private:
    MergeTreeData & storage;
    UInt32 version;

    std::vector<MutationCommand> commands;

    Logger * log;

    MergeTreeData::MutableDataPartPtr executeOnPart(const MergeTreeData::DataPartPtr & part, const Context & context) const;
};

}
