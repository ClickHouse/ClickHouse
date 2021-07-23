#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>


#include <Storages/StorageReplicatedMergeTree.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int NO_REPLICA_HAS_PART;
    extern const int ABORTED;
    extern const int PART_IS_TEMPORARILY_LOCKED;
}


bool ReplicatedMergeMutateTaskBase::execute()
{
    std::exception_ptr saved_exception;

    try
    {
        /// We don't have any backoff for failed entries
        /// we just count amount of tries for each of them.

        try
        {
            /// Returns false if
            if (executeImpl())
                return true;

            if (state == State::SUCCESS)
                storage.queue.removeProcessedEntry(storage.getZooKeeper(), selected_entry->log_entry);

            return false;
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
            {
                /// If no one has the right part, probably not all replicas work; We will not write to log with Error level.
                LOG_INFO(log, e.displayText());
            }
            else if (e.code() == ErrorCodes::ABORTED)
            {
                /// Interrupted merge or downloading a part is not an error.
                LOG_INFO(log, e.message());
            }
            else if (e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
            {
                /// Part cannot be added temporarily
                LOG_INFO(log, e.displayText());
                storage.cleanup_thread.wakeup();
            }
            else
                tryLogCurrentException(log, __PRETTY_FUNCTION__);

            /** This exception will be written to the queue element, and it can be looked up using `system.replication_queue` table.
                 * The thread that performs this action will sleep a few seconds after the exception.
                 * See `queue.processEntry` function.
                 */
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


    if (saved_exception)
    {
        std::lock_guard lock(storage.queue.state_mutex);
        selected_entry->log_entry->exception = saved_exception;
    }


    return false;
}



bool ReplicatedMergeMutateTaskBase::executeImpl()
{
    MemoryTrackerThreadSwitcherPtr switcher;
    if (merge_mutate_entry)
        switcher = std::make_unique<MemoryTrackerThreadSwitcher>(&(*merge_mutate_entry)->memory_tracker);

    switch (state)
    {
        case State::NEED_PREPARE :
        {
            if (!prepare())
            {
                state = State::CANT_MERGE_NEED_FETCH;
                return true;
            }

            /// Depending on condition there is no need to execute a merge
            if (state == State::SUCCESS)
                return false;

            state = State::NEED_EXECUTE_INNER_MERGE;
            return true;
        }
        case State::NEED_EXECUTE_INNER_MERGE :
        {
            try
            {
                if (!executeInnerTask())
                {
                    state = State::NEED_COMMIT;
                    return true;
                }
            }
            catch (...)
            {
                /// Maybe part is nullptr here?
                write_part_log(ExecutionStatus::fromCurrentException());
                throw;
            }

            return true;
        }
        case State::NEED_COMMIT :
        {
            try
            {
                if (!finalize())
                {
                    state = State::CANT_MERGE_NEED_FETCH;
                    return true;
                }
            }
            catch (...)
            {
                write_part_log(ExecutionStatus::fromCurrentException());
                throw;
            }

            state = State::SUCCESS;
            return true;
        }
        case State::CANT_MERGE_NEED_FETCH :
        {
            if (storage.executeFetch(entry))
            {
                state = State::SUCCESS;
                return true;
            }

            return false;
        }
        case State::SUCCESS :
        {
            /// Do nothing
            return false;
        }
    }
    return false;
}



}
