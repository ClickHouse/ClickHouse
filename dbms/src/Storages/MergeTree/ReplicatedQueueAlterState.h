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
        std::map<String, Int64> block_numbers;
        bool metadata_finished = false;

        AlterInQueue() = default;

        AlterInQueue(const std::map<String, Int64> & block_numbers_, bool metadata_finished_)
            : block_numbers(block_numbers_)
            , metadata_finished(metadata_finished_)
        {
        }
    };
    Poco::Logger * log;


public:
    AlterSequence(Poco::Logger * log_)
        : log(log_)
    {
    }
    std::map<int, AlterInQueue> queue_state;

    bool empty() const {
        return queue_state.empty();
    }

    void addMutationForAlter(int alter_version, const std::map<String, Int64> & block_numbers, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        LOG_DEBUG(log, "Adding mutation with alter version:" << alter_version);
        queue_state.emplace(alter_version, AlterInQueue(block_numbers, true));
    }

    void addMetadataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {
        LOG_DEBUG(log, "Adding meta with alter version:" << alter_version);
        if (!queue_state.count(alter_version))
            queue_state.emplace(alter_version, AlterInQueue({}, false));
        else
            queue_state[alter_version].metadata_finished = false;
    }

    bool canExecuteGetEntry(const String & part_name, MergeTreeDataFormatVersion format_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        if (empty())
            return true;

        MergeTreePartInfo info = MergeTreePartInfo::fromPartName(part_name, format_version);
        if (queue_state.begin()->second.block_numbers.count(info.partition_id))
            return info.getDataVersion() < queue_state.begin()->second.block_numbers.at(info.partition_id);
        return true;

    }

    void finishMetadataAlter(int alter_version, bool have_data_alter, std::unique_lock <std::mutex> & /*state_lock*/)
    {

        if (queue_state.empty())
        {
            throw Exception("Queue shouldn't be empty on metadata alter", ErrorCodes::LOGICAL_ERROR);
        }

        if (queue_state.begin()->first != alter_version)
        {
            LOG_DEBUG(log, "Finished metadata alter with version " + std::to_string(alter_version) + " but current alter in queue is " + std::to_string(queue_state.begin()->first));
            throw Exception("Finished metadata alter with version " + std::to_string(alter_version) + " but current alter in queue is " + std::to_string(queue_state.begin()->first), ErrorCodes::LOGICAL_ERROR);
        }

        if (!have_data_alter)
        {
            queue_state.erase(alter_version);
        }
        else
        {
            queue_state[alter_version].metadata_finished = true;
        }
    }

    void finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/)
    {

        /// queue can be empty after load of finished mutation without move of mutation pointer
        if (queue_state.empty())
        {
            LOG_DEBUG(log, "FINISHING DATA ALTER WITH VERSION:" << alter_version << " BUT QUEUE EMPTY");

            return;
        }

        LOG_DEBUG(log, "FINISH DATA ALTER: " << alter_version);
        queue_state.erase(alter_version);
    }

    bool canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        LOG_DEBUG(log, "Can execute data alter:" << alter_version);
        for (auto [key, value] : queue_state)
        {
            LOG_DEBUG(log, "Key:" << key << " is metadata finished:" << value.metadata_finished);
        }
        return queue_state.at(alter_version).metadata_finished;
    }
    bool canExecuteMetaAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const
    {
        return queue_state.empty() || queue_state.begin()->first == alter_version;
    }

};

}
