#pragma once

#include <deque>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ReplicatedQueueAlterChain
{
private:
    struct AlterState
    {
        bool metadata_finished = false;
        bool data_finished = false;

        AlterState() = default;

        AlterState(bool metadata_finished_, bool data_finished_)
            : metadata_finished(metadata_finished_)
            , data_finished(data_finished_)
        {
        }
    };

private:
    std::map<int, AlterState> queue_state;
public:

    int getHeadAlterVersion(std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        if (!queue_state.empty())
            return queue_state.begin()->first;
        return -1;
    }

    void addMutationForAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        if (!queue_state.count(alter_version))
            queue_state.emplace(alter_version, AlterState{true, false});
        else
            queue_state[alter_version].data_finished = false;
    }

    void addMetadataAlter(int alter_version, bool have_mutation, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        if (!queue_state.count(alter_version))
            queue_state.emplace(alter_version, AlterState{false, !have_mutation});
        else
            queue_state[alter_version].metadata_finished = false;
    }

    void finishMetadataAlter(int alter_version, std::unique_lock <std::mutex> & /*state_lock*/)
    {
        assert(!queue_state.empty());
        assert(queue_state.begin()->first == alter_version);

        if (queue_state[alter_version].data_finished)
            queue_state.erase(alter_version);
        else
            queue_state[alter_version].metadata_finished = true;
    }

    void finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        /// queue can be empty after load of finished mutation without move of mutation pointer
        if (queue_state.empty())
            return;

        if (alter_version >= queue_state.begin()->first)
        {
            assert(queue_state.count(alter_version));
            if (queue_state[alter_version].metadata_finished)
                queue_state.erase(alter_version);
            else
                queue_state[alter_version].data_finished = true;
        }
    }

    bool canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        if (!queue_state.count(alter_version))
            return true;

        return queue_state.at(alter_version).metadata_finished;
    }

    bool canExecuteMetaAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        if (queue_state.empty())
            return true;

        return queue_state.begin()->first == alter_version;
    }

};

}
