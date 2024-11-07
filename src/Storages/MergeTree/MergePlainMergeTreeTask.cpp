#include <Storages/MergeTree/MergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Interpreters/TransactionLog.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadFuzzer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageID MergePlainMergeTreeTask::getStorageID() const
{
    return storage.getStorageID();
}

void MergePlainMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}

bool MergePlainMergeTreeTask::executeStep()
{
    /// All metrics will be saved in the thread_group, including all scheduled tasks.
    /// In profile_counters only metrics from this thread will be saved.
    ProfileEventsScope profile_events_scope(&profile_counters);

    /// Make out memory tracker a parent of current thread memory tracker
    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_list_entry)
    {
        switcher.emplace((*merge_list_entry)->thread_group);
    }

    switch (state)
    {
        case State::NEED_PREPARE :
        {
            prepare();
            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE :
        {
            try
            {
                if (merge_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                write_part_log(ExecutionStatus::fromCurrentException("", true));
                throw;
            }
        }
        case State::NEED_FINISH :
        {
            finish();

            state = State::SUCCESS;
            return false;
        }
        case State::SUCCESS:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task with state SUCCESS mustn't be executed again");
        }
    }
}


void MergePlainMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();

    task_context = createTaskContext();
    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        task_context);

    storage.writePartLog(
        PartLogElement::MERGE_PARTS_START, {}, 0,
        future_part->name, new_part, future_part->parts, merge_list_entry.get(), {});

    write_part_log = [this] (const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        merge_task.reset();
        storage.writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch_ptr->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot));
    };

    transfer_profile_counters_to_initial_query = [this, query_thread_group = CurrentThread::getGroup()] ()
    {
        if (query_thread_group)
        {
            auto task_thread_group = (*merge_list_entry)->thread_group;
            auto task_counters_snapshot = task_thread_group->performance_counters.getPartiallyAtomicSnapshot();

            auto & query_counters = query_thread_group->performance_counters;
            for (ProfileEvents::Event i = ProfileEvents::Event(0); i < ProfileEvents::end(); ++i)
                query_counters.incrementNoTrace(i, task_counters_snapshot[i]);
        }
    };

    merge_task = storage.merger_mutator.mergePartsToTemporaryPart(
        future_part,
        metadata_snapshot,
        merge_list_entry.get(),
        {} /* projection_merge_list_element */,
        table_lock_holder,
        time(nullptr),
        task_context,
        merge_mutate_entry->tagger->reserved_space,
        deduplicate,
        deduplicate_by_columns,
        cleanup,
        storage.merging_params,
        txn);
}


void MergePlainMergeTreeTask::finish()
{
    new_part = merge_task->getFuture().get();

    MergeTreeData::Transaction transaction(storage, txn.get());
    storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction);
    transaction.commit();

    ThreadFuzzer::maybeInjectSleep();
    ThreadFuzzer::maybeInjectMemoryLimitException();

    if (auto * mark_cache = storage.getContext()->getMarkCache().get())
    {
        auto marks = merge_task->releaseCachedMarks();
        addMarksToCache(*new_part, marks, mark_cache);
    }

    write_part_log({});
    StorageMergeTree::incrementMergedPartsProfileEvent(new_part->getType());
    transfer_profile_counters_to_initial_query();

    if (auto txn_ = txn_holder.getTransaction())
    {
        /// Explicitly commit the transaction if we own it (it's a background merge, not OPTIMIZE)
        TransactionLog::instance().commitTransaction(txn_, /* throw_on_unknown_status */ false);
        ThreadFuzzer::maybeInjectSleep();
        ThreadFuzzer::maybeInjectMemoryLimitException();
    }
}

ContextMutablePtr MergePlainMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext());
    context->makeQueryContextForMerge(*storage.getSettings());
    auto queryId = getQueryId();
    context->setCurrentQueryId(queryId);
    context->setBackgroundOperationTypeForContext(ClientInfo::BackgroundOperationType::MERGE);
    return context;
}

}
