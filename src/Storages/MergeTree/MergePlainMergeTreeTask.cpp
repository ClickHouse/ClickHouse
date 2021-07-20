#include <Storages/MergeTree/MergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>


namespace DB
{


bool MergePlainMergeTreeTask::execute()
{
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
                if (merge_task->execute()) {
                    return true;
                }

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
            return false;
        }
    }
    return false;
}



void MergePlainMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();

    merge_list_entry = storage.getContext()->getMergeList().insert(storage.getStorageID(), future_part);

    write_part_log = [this] (const ExecutionStatus & execution_status)
    {
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
            *(merge_list_entry),
            table_lock_holder,
            time(nullptr),
            storage.getContext(),
            merge_mutate_entry->tagger->reserved_space,
            deduplicate,
            deduplicate_by_columns,
            storage.merging_params);
}



void MergePlainMergeTreeTask::finish()
{
    new_part = merge_task->getFuture().get();
    storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, nullptr);
    write_part_log({});
}

}
