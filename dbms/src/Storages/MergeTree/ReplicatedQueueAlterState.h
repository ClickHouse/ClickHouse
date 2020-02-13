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

class AlterSequence
{
private:
    struct AlterInQueue
    {
        bool metadata_finished = false;
        bool data_finished = false;

        AlterInQueue() = default;

        AlterInQueue(bool metadata_finished_, bool data_finished_)
            : metadata_finished(metadata_finished_)
            , data_finished(data_finished_)
        {
        }
    };

    std::map<int, AlterInQueue> queue_state;

public:
    bool empty() const {
        return queue_state.empty();
    }

    void addMutationForAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        if (!queue_state.count(alter_version))
            queue_state.emplace(alter_version, AlterInQueue(true, false));
        else
            queue_state[alter_version].data_finished = false;
    }

    void addMetadataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        if (!queue_state.count(alter_version))
            queue_state.emplace(alter_version, AlterInQueue(false, true));
        else
            queue_state[alter_version].metadata_finished = false;
    }

    void finishMetadataAlter(int alter_version, bool have_mutation, std::unique_lock <std::mutex> & /*state_lock*/)
    {
        assert(!queue_state.empty());
        assert(queue_state.begin()->first == alter_version);

        if (!have_mutation)
            queue_state.erase(alter_version);
        else
            queue_state[alter_version].metadata_finished = true;
    }

    void finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        /// queue can be empty after load of finished mutation without move of mutation pointer
        if (queue_state.empty())
            return;
        assert(queue_state.count(alter_version));

        if (queue_state[alter_version].metadata_finished)
            queue_state.erase(alter_version);
        else
            queue_state[alter_version].data_finished = true;
    }

    bool canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        if (!queue_state.count(alter_version))
            return true;
        return queue_state.at(alter_version).metadata_finished;
    }

    bool canExecuteMetaAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        return queue_state.empty() || queue_state.begin()->first == alter_version;
    }

};

}
