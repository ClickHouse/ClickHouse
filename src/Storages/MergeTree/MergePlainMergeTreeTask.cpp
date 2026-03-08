#include <Storages/MergeTree/MergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Interpreters/TransactionLog.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadFuzzer.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/Context.h>


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
    auto component_guard = Coordination::setCurrentComponent("MergePlainMergeTreeTask::executeStep");

    /// Make out memory tracker a parent of current thread memory tracker
    ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);

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
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception is in merge_task.");
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

    task_context = createTaskContext();
    thread_group = ThreadGroup::createForBackgroundOps(task_context);

    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        thread_group);

    storage.writePartLog(
        PartLogElement::MERGE_PARTS_START, {}, 0,
        future_part->name, new_part, future_part->parts, merge_list_entry.get(), {});

    write_part_log = [this] (const ExecutionStatus & execution_status)
    {
        LOG_DEBUG(storage.log, "Writing part log for merge of part {} with status {} duration {} ms",
            future_part->name, execution_status.message, thread_group->getGroupElapsedMs());

        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(thread_group->performance_counters.getPartiallyAtomicSnapshot());
        storage.writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            thread_group->getGroupElapsedNs(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot));
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

    auto prewarm_caches = storage.getCachesToPrewarm(new_part->getBytesUncompressedOnDisk());

    if (prewarm_caches.mark_cache)
    {
        auto marks = merge_task->releaseCachedMarks();
        addMarksToCache(*new_part, marks, prewarm_caches.mark_cache.get());
    }

    if (prewarm_caches.index_mark_cache)
    {
        auto index_marks = merge_task->releaseCachedIndexMarks();
        addMarksToCache(*new_part, index_marks, prewarm_caches.index_mark_cache.get());
    }

    if (prewarm_caches.primary_index_cache)
    {
        /// Move index to cache and reset it here because we need
        /// a correct part name after rename for a key of cache entry.
        new_part->moveIndexToCache(*prewarm_caches.primary_index_cache);
    }

    write_part_log({});

    StorageMergeTree::incrementMergedPartsProfileEvent(new_part->getType());

    if (auto txn_ = txn_holder.getTransaction())
    {
        /// Explicitly commit the transaction if we own it (it's a background merge, not OPTIMIZE)
        TransactionLog::instance().commitTransaction(txn_, /* throw_on_unknown_status */ false);
        ThreadFuzzer::maybeInjectSleep();
        ThreadFuzzer::maybeInjectMemoryLimitException();
    }

    merge_mutate_entry->finalize();
}

void MergePlainMergeTreeTask::cancel() noexcept
{
    auto component_guard = Coordination::setCurrentComponent("MergePlainMergeTreeTask::cancel");
    if (merge_task)
        merge_task->cancel();

    if (new_part)
        new_part->removeIfNeeded();

    /// We need to destroy task here because it holds RAII wrapper for
    /// temp directories which guards temporary dir from background removal which can
    /// conflict with the next scheduled merge because it will be possible after merge_mutate_entry->finalize()
    merge_task.reset();

    if (merge_mutate_entry)
        merge_mutate_entry->finalize();
}

ContextMutablePtr MergePlainMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext()->getBackgroundContext());
    context->makeQueryContextForMerge(*storage.getSettings());
    context->setCurrentQueryId(getQueryId());
    return context;
}

MergePlainMergeTreeTask::MergePlainMergeTreeTask(
    StorageMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    bool deduplicate_,
    Names deduplicate_by_columns_,
    bool cleanup_,
    MergeMutateSelectedEntryPtr merge_mutate_entry_,
    TableLockHolder table_lock_holder_,
    IExecutableTask::TaskResultCallback & task_result_callback_)
    : storage(storage_)
    , metadata_snapshot(std::move(metadata_snapshot_))
    , deduplicate(deduplicate_)
    , deduplicate_by_columns(std::move(deduplicate_by_columns_))
    , cleanup(cleanup_)
    , merge_mutate_entry(std::move(merge_mutate_entry_))
    , table_lock_holder(std::move(table_lock_holder_))
    , task_result_callback(task_result_callback_)
{
    for (auto & item : merge_mutate_entry->future_part->parts)
        priority.value += item->getBytesOnDisk();
}
}
