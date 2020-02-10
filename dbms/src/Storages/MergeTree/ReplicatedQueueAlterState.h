#pragma once

#include <deque>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>

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
        DATA_CHANGES_NOT_NEEDED,
    };

    struct AlterInQueue
    {
        int alter_version;
        AlterState state;
        std::map<String, Int64> block_numbers;

        AlterInQueue(int alter_version_, AlterState state_)
            : alter_version(alter_version_)
            , state(state_)
        {
        }
    };
    Poco::Logger * log;


public:
    AlterSequence(Poco::Logger * log_)
        : log(log_)
    {
    }
    std::deque<AlterInQueue> queue;

    bool empty() const {
        return queue.empty();
    }

    void addMetadataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        if (!queue.empty() && queue.front().alter_version > alter_version)
        {
            throw Exception("Alter not in order " + std::to_string(alter_version), ErrorCodes::LOGICAL_ERROR);
        }
        queue.emplace_back(alter_version, AlterState::APPLY_METADATA_CHANGES);
    }

    void addDataAlterIfEmpty(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        if (queue.empty())
            queue.emplace_back(alter_version, AlterState::APPLY_DATA_CHANGES);

        //if (queue.front().alter_version != alter_version)
        //{
        //    throw Exception(
        //        "Alter head has another version number "
        //        + std::to_string(queue.front().alter_version) + " than ours " + std::to_string(alter_version),
        //        ErrorCodes::LOGICAL_ERROR);
        //}
    }

    void finishMetadataAlter(int alter_version, bool have_data_alter, std::unique_lock <std::mutex> & /*state_lock*/)
    {

        if (queue.empty())
        {
            throw Exception("Queue shouldn't be empty on metadata alter", ErrorCodes::LOGICAL_ERROR);
        }

        LOG_DEBUG(
            log,
            "FINISHING METADATA ALTER WITH VERSION:" << alter_version << " AND HAVE DATA ALTER: " << have_data_alter
                                                     << " QUEUE HEAD:" << queue.front().alter_version << " state:" << queue.front().state);
        if (queue.front().alter_version != alter_version)
            throw Exception("Finished metadata alter with version " + std::to_string(alter_version) + " but current alter in queue is " + std::to_string(queue.front().alter_version), ErrorCodes::LOGICAL_ERROR);

        if (have_data_alter && queue.front().state == AlterState::APPLY_METADATA_CHANGES)
        {
            LOG_DEBUG(
                log,
                "FINISHING METADATA ALTER WITH VERSION:" << alter_version << " AND SWITCHING QUEUE STATE");

            //std::cerr << "Switching head state:" << AlterState::APPLY_DATA_CHANGES << std::endl;
            queue.front().state = AlterState::APPLY_DATA_CHANGES;
        }
        else
        {
            LOG_DEBUG(log, "FINISHING METADATA ALTER WITH VERSION:" << alter_version << " AND DOING POP");

            //std::cerr << "JUST POP FRONT\n";
            queue.pop_front();
        }
    }

    void finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {

        /// queue can be empty after load of finished mutation without move of mutation pointer
        if (queue.empty())
        {
            LOG_DEBUG(log, "FINISHING DATA ALTER WITH VERSION:" << alter_version << " BUT QUEUE EMPTY");

            return;
        }
        //std::cerr << "Finishing data alter:" << alter_version << std::endl;
        if (queue.front().alter_version != alter_version)
        {
            for (auto & state : queue)
            {
                if (state.alter_version == alter_version)
                {
                    LOG_DEBUG(log, "FINISHING DATA ALTER WITH VERSION:" << alter_version << " BUT HEAD IS NOT SAME SO MAKE DATA_CHANGED_NOT_NEEDED");
                    state.state = AlterState::DATA_CHANGES_NOT_NEEDED;
                    return;
                }
            }
        }
        //if (queue.front().alter_version != alter_version)
        //{
        //    LOG_DEBUG(log, "FINISHING DATA ALTER WITH VERSION:" << alter_version << " BUT QUEUE VERSION IS " << queue.front().alter_version << " state " << queue.front().state);
        //    throw Exception(
        //        "Finished data alter with version " + std::to_string(alter_version) + " but current alter in queue is "
        //            + std::to_string(queue.front().alter_version),
        //        ErrorCodes::LOGICAL_ERROR);
        //}

        if (queue.front().state != AlterState::APPLY_DATA_CHANGES)
        {
            LOG_DEBUG(
                log, "FINISHING DATA ALTER WITH VERSION:" << alter_version << " BUT STATE IS METADATA");
            queue.front().state = AlterState::DATA_CHANGES_NOT_NEEDED;
            return;
        }

        LOG_DEBUG(
            log,
            "FINISHING DATA ALTER WITH VERSION:" << alter_version << " QUEUE VERSION IS " << queue.front().alter_version << " STATE "
                                                 << queue.front().state);
        queue.pop_front();
    }

    bool canExecuteMetadataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        if (queue.empty())
            throw Exception("QUEUE EMPTY ON METADATA", ErrorCodes::LOGICAL_ERROR);
        LOG_DEBUG(log, "CHECK METADATADATA ALTER WITH VERSION:" << alter_version << " BUT QUEUE HEAD IS " << queue.front().alter_version);

        //std::cerr << "Alter queue front:" << queue.front().alter_version << " state:" << queue.front().state << std::endl;
        return queue.front().alter_version == alter_version;
    }

    bool canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        if (queue.empty())
            throw Exception("QUEUE EMPTY ON DATA", ErrorCodes::LOGICAL_ERROR);
        //std::cerr << "Alter queue front:" << queue.front().alter_version << " state:" << queue.front().state << std::endl;
        //std::cerr << "CAn execute:" << alter_version << std::endl;
        //std::cerr << "FRont version:" << queue.front().alter_version << std::endl;
        //std::cerr << "State:" << queue.front().state << std::endl;
        LOG_DEBUG(log, "CHECK DATA ALTER WITH VERSION:" << alter_version << " BUT QUEUE HEAD IS " << queue.front().alter_version << " state:" << queue.front().state);
        return queue.front().alter_version == alter_version && queue.front().state == AlterState::APPLY_DATA_CHANGES;
    }

};

}
