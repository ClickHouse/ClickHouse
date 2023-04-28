#include <Storages/MergeTree/MergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ProfileEvents.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MERGE_ABORT;
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
    return false;
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

    if (storage.merging_params.mode == MergeTreeData::MergingParams::Mode::Unique)
    {
        auto write_state = merge_task->getWriteState();

        std::lock_guard<std::mutex> write_merge_lock(storage.write_merge_lock);

        /// such that we can reduce the time of lockParts()
        std::unordered_set<UInt32> deleted_rows_set;
        {
            std::unordered_set<String> deleted_keys;
            /// Here we hold the write_merge_lock, and finish the merge task, in this period write will stop
            for (const auto & merged_part : future_part->parts)
            {
                if (auto it = storage.delete_buffer->find(merged_part->info); it != storage.delete_buffer->end())
                {
                    deleted_keys.insert(it->second.begin(), it->second.end());
                    storage.delete_buffer->erase(it);
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

            auto new_delete_bitmap = std::make_shared<DeleteBitmap>();

            auto new_version = storage.table_version->get()->version + 1;
            new_delete_bitmap->setVersion(new_version);
            new_delete_bitmap->addDels(deleted_rows);

            new_delete_bitmap->serialize(new_part->getDataPartStoragePtr());
            auto & delete_bitmap_cache = storage.delete_bitmap_cache;
            delete_bitmap_cache->set({part_info, new_version}, new_delete_bitmap);
            deleted_rows_set.insert(deleted_rows.begin(), deleted_rows.end());
        }
        auto partition_id = new_part->info.partition_id;
        auto primary_index = storage.primary_index_cache->getOrCreate(partition_id, new_part->partition);
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

        auto new_table_version = std::make_unique<TableVersion>(*storage.table_version->get());
        new_table_version->version++;

        if (!new_table_version->part_versions.insert({new_part->info, new_table_version->version}).second)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Insert new inserted part version into table version failed, this is a bug, new part name: {}",
                new_part->info.getPartNameForLogs());
        }
        MergeTreeData::Transaction transaction(storage, txn.get());
        storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction, std::move(new_table_version));
        transaction.commit();

        if (has_update)
            primary_index->setState(PrimaryIndex::State::UPDATED);
    }

    else
    {
        MergeTreeData::Transaction transaction(storage, txn.get());
        storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction);
        transaction.commit();
    }

    write_part_log({});
    storage.incrementMergedPartsProfileEvent(new_part->getType());
    transfer_profile_counters_to_initial_query();
}

ContextMutablePtr MergePlainMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext());
    context->makeQueryContext();
    auto queryId = storage.getStorageID().getShortName() + "::" + future_part->name;
    context->setCurrentQueryId(queryId);
    return context;
}

}
