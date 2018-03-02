#pragma once

#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <common/logger_useful.h>


namespace DB
{

class MergeTreeMutation
{
public:
    MergeTreeMutation(MergeTreeData & data_, UInt32 version_, std::vector<MutationCommand> commands_);

    void execute(const Context & context);

private:
    MergeTreeData & data;
    UInt32 version;

    std::vector<MutationCommand> commands;

    struct MutationAction
    {
        std::function<void(BlockInputStreamPtr &)> stream_transform;
    };

    Logger * log;

    MergeTreeData::MutableDataPartPtr executeOnPart(
        const MergeTreeData::DataPartPtr & part,
        const std::vector<MutationAction> & actions,
        const Context & context) const;
};

}
