#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ErrorCodes.h>
#include <Common/ProfileEventsScope.h>


namespace DB
{
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 max_postpone_time_for_failed_mutations_ms;
}

namespace ErrorCodes
{
    extern const int NO_REPLICA_HAS_PART;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
    extern const int PART_IS_TEMPORARILY_LOCKED;
}

StorageID ReplicatedMergeMutateTaskBase::getStorageID() const
{
    return storage.getStorageID();
}

void ReplicatedMergeMutateTaskBase::onCompleted()
{
    bool successfully_executed = state == State::SUCCESS;
    task_result_callback(successfully_executed);
}


bool ReplicatedMergeMutateTaskBase::executeStep()
{
    /// Metrics will be saved in the local profile_counters.
    ProfileEventsScope profile_events_scope(&profile_counters);

    std::exception_ptr saved_exception;

    bool retryable_error = false;
    try
    {
        /// We don't have any backoff for failed entries
        /// we just count amount of tries for each of them.

        try
        {
            return executeImpl();
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
            {
                /// If no one has the right part, probably not all replicas work; We will not write to log with Error level.
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                retryable_error = true;
            }
            else if (e.code() == ErrorCodes::ABORTED)
            {
                /// Interrupted merge or downloading a part is not an error.
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                retryable_error = true;
            }
            else if (e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
            {
                /// Part cannot be added temporarily
                LOG_INFO(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
                retryable_error = true;
                storage.cleanup_thread.wakeup();
            }
            else
                tryLogCurrentException(log, __PRETTY_FUNCTION__);

            /// This exception will be written to the queue element, and it can be looked up using `system.replication_queue` table.
            /// The thread that performs this action will sleep a few seconds after the exception.
            /// See `queue.processEntry` function.
            throw;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            throw;
        }

    }
    catch (...)
    {
        saved_exception = std::current_exception();
    }

    if (!retryable_error && saved_exception)
    {
        std::lock_guard lock(storage.queue.state_mutex);

        auto & log_entry = selected_entry->log_entry;

        log_entry->exception = saved_exception;
        log_entry->last_exception_time = time(nullptr);

        if (log_entry->type == ReplicatedMergeTreeLogEntryData::MUTATE_PART)
        {
            /// Record the exception in the system.mutations table.
            Int64 result_data_version = MergeTreePartInfo::fromPartName(log_entry->new_part_name, storage.queue.format_version)
                .getDataVersion();
            auto source_part_info = MergeTreePartInfo::fromPartName(
                log_entry->source_parts.at(0), storage.queue.format_version);

            auto in_partition = storage.queue.mutations_by_partition.find(source_part_info.partition_id);
            if (in_partition != storage.queue.mutations_by_partition.end())
            {
                auto mutations_begin_it = in_partition->second.upper_bound(source_part_info.getDataVersion());
                auto mutations_end_it = in_partition->second.upper_bound(result_data_version);
                for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
                {
                    auto & src_part = log_entry->source_parts.at(0);
                    ReplicatedMergeTreeQueue::MutationStatus & status = *it->second;
                    status.latest_failed_part = src_part;
                    status.latest_failed_part_info = source_part_info;
                    status.latest_fail_time = time(nullptr);
                    status.latest_fail_reason = getExceptionMessage(saved_exception, false);
                    status.latest_fail_error_code_name = ErrorCodes::getName(getExceptionErrorCode(saved_exception));
                    if (result_data_version == it->first)
                        storage.mutation_backoff_policy.addPartMutationFailure(src_part, (*storage.getSettings())[MergeTreeSetting::max_postpone_time_for_failed_mutations_ms]);
                }
            }
        }
    }

    if (retryable_error)
        print_exception = false;

    if (saved_exception)
        std::rethrow_exception(saved_exception);

    return false;
}


bool ReplicatedMergeMutateTaskBase::executeImpl()
{
    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_mutate_entry)
        switcher.emplace((*merge_mutate_entry)->thread_group);

    auto remove_processed_entry = [&] () -> bool
    {
        try
        {
            storage.queue.removeProcessedEntry(storage.getZooKeeper(), selected_entry->log_entry);
            state = State::SUCCESS;

            auto & log_entry = selected_entry->log_entry;
            if (log_entry->type == ReplicatedMergeTreeLogEntryData::MUTATE_PART)
            {
                storage.mutation_backoff_policy.removePartFromFailed(log_entry->source_parts.at(0));
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        return false;
    };


    auto execute_fetch = [&] (bool need_to_check_missing_part) -> bool
    {
        try
        {
            if (storage.executeFetch(entry, need_to_check_missing_part))
                return remove_processed_entry();
        }
        catch (...)
        {
            part_log_writer(ExecutionStatus::fromCurrentException("", true));
            throw;
        }

        return false;
    };


    switch (state)
    {
        case State::NEED_PREPARE :
        {
            {
                auto res = checkExistingPart();
                /// Depending on condition there is no need to execute a merge
                if (res == CheckExistingPartResult::PART_EXISTS)
                    return remove_processed_entry();
            }

            auto prepare_result = prepare();

            part_log_writer = prepare_result.part_log_writer;

            /// Avoid rescheduling, execute fetch here, in the same thread.
            if (!prepare_result.prepared_successfully)
                return execute_fetch(prepare_result.need_to_check_missing_part_in_fetch);

            state = State::NEED_EXECUTE_INNER_MERGE;
            return true;
        }
        case State::NEED_EXECUTE_INNER_MERGE :
        {
            try
            {
                if (!executeInnerTask())
                {
                    state = State::NEED_FINALIZE;
                    return true;
                }
            }
            catch (...)
            {
                part_log_writer(ExecutionStatus::fromCurrentException("", true));
                throw;
            }

            return true;
        }
        case State::NEED_FINALIZE :
        {
            if (!finalize(part_log_writer))
                return execute_fetch(/* need_to_check_missing = */true);

            return remove_processed_entry();
        }
        case State::SUCCESS :
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Do not call execute on previously succeeded task");
        }
    }
    return false;
}


ReplicatedMergeMutateTaskBase::CheckExistingPartResult ReplicatedMergeMutateTaskBase::checkExistingPart()
{
    /// If we already have this part or a part covering it, we do not need to do anything.
    /// The part may be still in the PreActive -> Active transition so we first search
    /// among PreActive parts to definitely find the desired part if it exists.
    MergeTreeData::DataPartPtr existing_part = storage.getPartIfExists(entry.new_part_name, {MergeTreeDataPartState::PreActive});

    if (!existing_part)
        existing_part = storage.getActiveContainingPart(entry.new_part_name);

    /// Even if the part is local, it (in exceptional cases) may not be in ZooKeeper. Let's check that it is there.
    if (existing_part && storage.getZooKeeper()->exists(fs::path(storage.replica_path) / "parts" / existing_part->name))
    {
        LOG_DEBUG(log, "Skipping action for part {} because part {} already exists.", entry.new_part_name, existing_part->name);

        /// We will exit from all the execution process
        return CheckExistingPartResult::PART_EXISTS;
    }


    return CheckExistingPartResult::OK;
}


}
