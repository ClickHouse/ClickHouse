#include <Storages/UniqueMergeTree/MergePlainUniqueMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/StorageUniqueMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MERGE_ABORT;
}


StorageID MergePlainUniqueMergeTreeTask::getStorageID()
{
    return storage.getStorageID();
}

void MergePlainUniqueMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}


bool MergePlainUniqueMergeTreeTask::executeStep()
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


void MergePlainUniqueMergeTreeTask::prepare()
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
        txn,
        nullptr,
        nullptr,
        "",
        &storage);
}


void MergePlainUniqueMergeTreeTask::finish()
{
    new_part = merge_task->getFuture().get();
    auto builder = merge_task->getBuilder();

    auto write_state = merge_task->getWriteState();

    std::lock_guard<std::mutex> write_merge_lock(storage.write_merge_lock);

    /// such that we can reduce the time of lockParts()
    std::unordered_set<UInt32> deleted_rows_set;
    {
        std::unordered_set<String> deleted_keys;
        /// Here we hold the write_merge_lock, and finish the merge task, in this period write will stop
        for (const auto & merged_part : future_part->parts)
        {
            if (auto it = storage.delete_buffer.find(merged_part->info); it != storage.delete_buffer.end())
            {
                deleted_keys.insert(it->second.begin(), it->second.end());
                storage.delete_buffer.erase(it);
            }
        }
        auto settings = storage.getSettings();
        if (deleted_keys.size() >= settings->unique_merge_tree_mininum_delete_buffer_size_to_abort_merge)
        {
            throw Exception(
                ErrorCodes::MERGE_ABORT,
                "Too much deletes written during merge, abort the merge, deleted keys number during merge: {}",
                deleted_keys.size());
        }
        auto part_info = new_part->info;
        std::vector<UInt32> deleted_rows;
        for (size_t row_id = 0; row_id < new_part->rows_count; ++row_id)
        {
            auto deleted_key = write_state.key_column->getDataAt(row_id).toString();
            if (deleted_keys.contains(deleted_key))
                deleted_rows.emplace_back(row_id);
        }

        auto part_data_path = new_part->data_part_storage->getRelativePath();
        auto new_delete_bitmap = std::make_shared<DeleteBitmap>();

        auto new_version = storage.currentVersion()->version + 1;
        new_delete_bitmap->setVersion(new_version);
        new_delete_bitmap->addDels(deleted_rows);

        new_delete_bitmap->serialize(part_data_path + StorageUniqueMergeTree::DELETE_DIR_NAME, new_part->data_part_storage->getDisk());
        auto & delete_bitmap_cache = storage.deleteBitmapCache();
        delete_bitmap_cache.set({part_info, new_version}, new_delete_bitmap);
        deleted_rows_set.insert(deleted_rows.begin(), deleted_rows.end());
    }
    auto partition_id = new_part->info.partition_id;
    auto primary_index = storage.primaryIndexCache().getOrCreate(partition_id, new_part->partition);
    /// Do not update primary index cache if the merged block is larger than cache size,
    /// just set the cache to be UPDATED.
    bool has_update = false;
    if (new_part->rows_count >= 0.3 * storage.getSettings()->unique_merge_tree_max_primary_index_cache_size)
    {
        /// In this case, after the merge confirmed, new insert must be construct primary index, since
        /// cache becoma invalid
        primary_index->setState(PrimaryIndex::State::UPDATED);
    }
    else
    {
        has_update = primary_index->update(new_part->info.min_block, write_state.key_column, deleted_rows_set);
        if (has_update)
            primary_index->setState(PrimaryIndex::State::UPDATED);
    }

    auto new_table_version = std::make_unique<TableVersion>(*storage.currentVersion());
    new_table_version->version++;

    if (!new_table_version->part_versions.insert({new_part->info, new_table_version->version}).second)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Insert new inserted part version into table version failed, this is a bug, new part name: {}",
            new_part->info.getPartName());
    }

    MergeTreeData::Transaction transaction(storage, txn.get());
    storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction, builder, std::move(new_table_version));
    transaction.commit();

    if (has_update)
        primary_index->setState(PrimaryIndex::State::UPDATED);

    write_part_log({});
    storage.currently_processing_in_background_condition.notify_all();
}

}
