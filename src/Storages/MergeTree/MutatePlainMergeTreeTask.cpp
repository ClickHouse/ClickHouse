#include <cstddef>
#include <Storages/MergeTree/MutatePlainMergeTreeTask.h>

#include <Storages/StorageMergeTree.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/Context.h>
#include <Common/ErrorCodes.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEventsScope.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_sharing_sets_for_mutations;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char mt_mutate_task_pause_before_prepare[];
}


StorageID MutatePlainMergeTreeTask::getStorageID() const
{
    return storage.getStorageID();
}

void MutatePlainMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}

bool MutatePlainMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;

    task_context = createTaskContext();
    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        task_context);

    /// The task is queued after `StorageMergeTree::scheduleDataProcessingJob` releases
    /// `currently_processing_in_background_mutex`, so `KILL TRANSACTION` can cancel the
    /// mutation's transaction between the selection and this point. The rollback's
    /// `killMutation` sweep only cancels tasks that are already in `MergeList`, so the check
    /// must happen after the insertion above: `MergeTreeTransaction::rollback` stores
    /// `Tx::RolledBackCSN` before the sweep, hence a rollback missed by this check finds the
    /// task in `MergeList` and cancels it there. Otherwise the task would run the whole
    /// mutate pass only to throw from `MergeTreeTransaction::checkIsNotCancelled` at commit.
    if (const auto & txn = merge_mutate_entry->txn; txn && txn->getCSN() == Tx::RolledBackCSN)
    {
        LOG_DEBUG(
            getLogger("MutatePlainMergeTreeTask"),
            "Skipping mutation of part {}: transaction {} was cancelled after the mutation was selected",
            future_part->name,
            txn->tid);
        return false;
    }

    stopwatch = std::make_unique<Stopwatch>();

    const auto & mutation_ids = merge_mutate_entry->mutation_ids;
    chassert(!mutation_ids.empty());

    storage.writePartLog(
        PartLogElement::MUTATE_PART_START, {}, 0,
        future_part->name, new_part, future_part->parts, merge_list_entry.get(), {}, mutation_ids, {});

    write_part_log = [this, mutation_ids] (const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        storage.writePartLog(
            PartLogElement::MUTATE_PART,
            execution_status,
            stopwatch->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot),
            mutation_ids, {});
    };

    if (task_context->getSettingsRef()[Setting::enable_sharing_sets_for_mutations])
    {
        /// If we have a prepared sets cache for this mutations, we will use it.
        auto mutation_id = future_part->part_info.mutation;
        auto prepared_sets_cache_for_mutation = storage.getPreparedSetsCache(mutation_id);
        task_context->setPreparedSetsCache(prepared_sets_cache_for_mutation);
    }

    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
            future_part, metadata_snapshot, merge_mutate_entry->commands, merge_list_entry.get(),
            time(nullptr), task_context, merge_mutate_entry->txn, merge_mutate_entry->tagger->reserved_space, table_lock_holder);
    return true;
}

void MutatePlainMergeTreeTask::finish()
{
    if (merge_mutate_entry)
        merge_mutate_entry->finalize();
}

bool MutatePlainMergeTreeTask::executeStep()
{
    auto component_guard = Coordination::setCurrentComponent("MutatePlainMergeTreeTask::executeStep");
    /// Metrics will be saved in the local profile_counters.
    ProfileEventsScope profile_events_scope(&profile_counters);

    /// Make out memory tracker a parent of current thread memory tracker
    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_list_entry)
        switcher.emplace((*merge_list_entry)->thread_group, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);

    switch (state)
    {
        case State::NEED_PREPARE:
        {
            FailPointInjection::pauseFailPoint(FailPoints::mt_mutate_task_pause_before_prepare);

            if (!prepare())
            {
                state = State::NEED_FINISH;
                return true;
            }

            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE:
        {
            try
            {
                if (mutate_task->execute())
                    return true;

                new_part = mutate_task->getFuture().get();
                auto & data_part_storage = new_part->getDataPartStorage();
#if CLICKHOUSE_CLOUD
                data_part_storage.setPreferredFileOrder(new_part->getPreferredFileOrder());
#endif
                if (data_part_storage.hasActiveTransaction())
                    data_part_storage.commitTransaction();

                MergeTreeData::Transaction transaction(storage, merge_mutate_entry->txn.get());
                /// Hold data_parts_lock across both renameTempPartAndReplace and commit to prevent
                /// a race with REPLACE PARTITION. Without this, there is a window where the mutation
                /// result is PreActive (not yet committed): REPLACE PARTITION's
                /// removePartsInRangeFromWorkingSet only removes Active parts and misses the PreActive
                /// mutation result. After REPLACE releases the lock, the mutation's commit promotes
                /// the PreActive part to Active, "resurrecting" old data.
                {
                    auto lock = storage.lockParts();
                    storage.renameTempPartAndReplaceUnlocked(new_part, transaction, lock, /*rename_in_transaction=*/ false);
                    transaction.commit(lock);
                }

                mutate_task->updateProfileEvents();

                /// Write the part log entry before reporting the mutation as done, otherwise a
                /// synchronous mutation (mutations_sync) may return to the client before the
                /// MutatePart row is queued, so a subsequent SYSTEM FLUSH LOGS misses it.
                write_part_log({});

                storage.updateMutationEntriesErrors(future_part, true, "", "");

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                if (merge_mutate_entry->txn)
                    merge_mutate_entry->txn->onException();
                PreformattedMessage exception_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false);
                LOG_ERROR(getLogger("MutatePlainMergeTreeTask"), exception_message);
                String error_code_name(ErrorCodes::getName(getCurrentExceptionCode()));
                mutate_task->updateProfileEvents();

                /// Same ordering as the success path: queue the failed part log entry before
                /// publishing the mutation error, otherwise a synchronous mutation (mutations_sync)
                /// may return to the client (it also unblocks on the failure reason) before the
                /// MutatePart row is queued, so a subsequent SYSTEM FLUSH LOGS misses it.
                write_part_log(ExecutionStatus::fromCurrentException("", true));

                storage.updateMutationEntriesErrors(future_part, false, exception_message.text, error_code_name);
                tryLogCurrentException(__PRETTY_FUNCTION__);
                throw;
            }
        }
        case State::NEED_FINISH:
        {
            // Nothing to do
            finish();
            state = State::SUCCESS;
            return false;
        }
        case State::SUCCESS:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task with state SUCCESS mustn't be executed again");
        }
    }

    return false;
}

void MutatePlainMergeTreeTask::cancel() noexcept
{
    auto component_guard = Coordination::setCurrentComponent("MutatePlainMergeTreeTask::cancel");
    if (mutate_task)
        mutate_task->cancel();

    if (new_part)
        new_part->removeIfNeeded();

    /// We need to destroy task here because it holds RAII wrapper for
    /// temp directories which guards temporary dir from background removal which can
    /// conflict with the next scheduled merge because it will be possible after merge_mutate_entry->finalize()
    mutate_task.reset();

    if (merge_mutate_entry)
        merge_mutate_entry->finalize();
}


ContextMutablePtr MutatePlainMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext()->getBackgroundContext());
    context->makeQueryContextForMutate(*storage.getSettings());
    auto queryId = getQueryId();
    context->setCurrentQueryId(queryId);
    return context;
}

}
