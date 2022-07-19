#include <Storages/MergeTree/MergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageID MergePlainMergeTreeTask::getStorageID()
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
    /// Make out memory tracker a parent of current thread memory tracker
    MemoryTrackerThreadSwitcherPtr switcher;
    if (merge_list_entry)
        switcher = std::make_unique<MemoryTrackerThreadSwitcher>(*merge_list_entry);

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
                write_part_log(ExecutionStatus::fromCurrentException());
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
    return false;
}


void MergePlainMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();

    const Settings & settings = storage.getContext()->getSettingsRef();
    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        settings);

    write_part_log = [this] (const ExecutionStatus & execution_status)
    {
        merge_task.reset();
        storage.writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch_ptr->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get());
    };

    merge_task = storage.merger_mutator.mergePartsToTemporaryPart(
            future_part,
            metadata_snapshot,
            merge_list_entry.get(),
            {} /* projection_merge_list_element */,
            table_lock_holder,
            time(nullptr),
            storage.getContext(),
            merge_mutate_entry->tagger->reserved_space,
            deduplicate,
            deduplicate_by_columns,
            storage.merging_params,
            txn);
}


void MergePlainMergeTreeTask::finish()
{
    new_part = merge_task->getFuture().get();
    auto builder = merge_task->getBuilder();

    MergeTreeData::Transaction transaction(storage, txn.get());
    storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction, builder);
    transaction.commit();

    write_part_log({});
    storage.incrementMergedPartsProfileEvent(new_part->getType());
}

}
