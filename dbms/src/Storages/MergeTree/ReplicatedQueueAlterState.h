#pragma once

#include <deque>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class AlterSequence
{
private:
    enum AlterState
    {
        APPLY_METADATA_CHANGES,
        APPLY_DATA_CHANGES,
    };

    struct AlterInQueue
    {
        int alter_version;
        AlterState state;

        AlterInQueue(int alter_version_, AlterState state_)
            : alter_version(alter_version_)
            , state(state_)
        {
        }
    };


    std::deque<AlterInQueue> queue;
public:

    bool empty() const {
        return queue.empty();
    }

    void addMetadataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        queue.emplace_back(alter_version, AlterState::APPLY_METADATA_CHANGES);
    }

    void addDataAlterIfEmpty(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        if (queue.empty())
            queue.emplace_back(alter_version, AlterState::APPLY_DATA_CHANGES);

        if (queue.front().alter_version != alter_version)
        {
            throw Exception(
                "Alter head has another version number "
                + std::to_string(queue.front().alter_version) + " than ours " + std::to_string(alter_version),
                ErrorCodes::LOGICAL_ERROR);
        }
    }

    void finishMetadataAlter(int alter_version, bool have_data_alter, std::unique_lock <std::mutex> & /*state_lock*/)
    {
        if (queue.empty())
        {
            throw Exception("Queue shouldn't be empty on metadata alter", ErrorCodes::LOGICAL_ERROR);
        }
        if (queue.front().alter_version != alter_version)
            throw Exception("Finished metadata alter with version " + std::to_string(alter_version) + " but current alter in queue is " + std::to_string(queue.front().alter_version), ErrorCodes::LOGICAL_ERROR);

        if (have_data_alter)
        {
            //std::cerr << "Switching head state:" << AlterState::APPLY_DATA_CHANGES << std::endl;
            queue.front().state = AlterState::APPLY_DATA_CHANGES;
        }
        else
        {
            //std::cerr << "JUST POP FRONT\n";
            queue.pop_front();
        }
    }

    void finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        /// queue can be empty after load of finished mutation without move of mutation pointer
        if (queue.empty())
            return;
        //std::cerr << "Finishing data alter:" << alter_version << std::endl;
        if (queue.front().alter_version != alter_version)
            throw Exception(
                "Finished data alter with version " + std::to_string(alter_version) + " but current alter in queue is "
                    + std::to_string(queue.front().alter_version),
                ErrorCodes::LOGICAL_ERROR);

        if (queue.front().state != AlterState::APPLY_DATA_CHANGES)
        {
            throw Exception(
                "Finished data alter but current alter should perform metadata alter",
                ErrorCodes::LOGICAL_ERROR);
        }

        queue.pop_front();
    }

    bool canExecuteMetadataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        //std::cerr << "Alter queue front:" << queue.front().alter_version << " state:" << queue.front().state << std::endl;
        return queue.front().alter_version == alter_version;
    }

    bool canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        //std::cerr << "CAn execute:" << alter_version << std::endl;
        //std::cerr << "FRont version:" << queue.front().alter_version << std::endl;
        //std::cerr << "State:" << queue.front().state << std::endl;
        return queue.front().alter_version == alter_version && queue.front().state == AlterState::APPLY_DATA_CHANGES;
    }

};

}
