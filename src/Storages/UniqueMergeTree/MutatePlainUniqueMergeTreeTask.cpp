#include <Storages/UniqueMergeTree/MutatePlainUniqueMergeTreeTask.h>

#include <Interpreters/TransactionLog.h>
#include <Storages/StorageUniqueMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageID MutatePlainUniqueMergeTreeTask::getStorageID()
{
    return storage.getStorageID();
}

void MutatePlainUniqueMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}


void MutatePlainUniqueMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;

    const Settings & settings = storage.getContext()->getSettingsRef();
    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        settings);

    stopwatch = std::make_unique<Stopwatch>();

    write_part_log = [this] (const ExecutionStatus & execution_status)
    {
        mutate_task.reset();
        storage.writePartLog(
            PartLogElement::MUTATE_PART,
            execution_status,
            stopwatch->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get());
    };

    fake_query_context = Context::createCopy(storage.getContext());
    fake_query_context->makeQueryContext();
    fake_query_context->setCurrentQueryId("");

    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
            future_part, metadata_snapshot, merge_mutate_entry->commands, merge_list_entry.get(),
            time(nullptr), fake_query_context, merge_mutate_entry->txn, merge_mutate_entry->tagger->reserved_space, table_lock_holder);
}

bool MutatePlainUniqueMergeTreeTask::executeStep()
{

    /// Make out memory tracker a parent of current thread memory tracker
    MemoryTrackerThreadSwitcherPtr switcher;
    if (merge_list_entry)
        switcher = std::make_unique<MemoryTrackerThreadSwitcher>(*merge_list_entry);

    switch (state)
    {
        case State::NEED_PREPARE:
        {
            prepare();
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

                auto builder = mutate_task->getBuilder();
                if (!builder)
                    builder = new_part->data_part_storage->getBuilder();

                {
                    std::lock_guard<std::mutex> lock(storage.write_merge_lock);
                    /// Here we should not increment the version of table version, because we do not modify delete bitmap
                    auto new_table_version = std::make_unique<TableVersion>(*storage.currentVersion());
                    auto old_part_info = merge_mutate_entry->future_part->parts[0]->info;
                    auto new_part_version = new_table_version->part_versions.at(old_part_info);

                    if (storage.delete_buffer.contains(merge_mutate_entry->future_part->parts[0]->info)
                        || !new_part->data_part_storage->exists(
                            StorageUniqueMergeTree::DELETE_DIR_NAME + toString(new_part_version) + ".bitmap"))
                    {
                        auto bitmap = storage.delete_bitmap_cache.getOrCreate(merge_mutate_entry->future_part->parts[0], new_part_version);
                        auto disk = new_part->data_part_storage->getDisk();
                        bitmap->serialize(
                            new_part->data_part_storage->getRelativePath() + StorageUniqueMergeTree::DELETE_DIR_NAME,
                            new_part->data_part_storage->getDisk());
                        if (storage.delete_buffer.contains(merge_mutate_entry->future_part->parts[0]->info))
                            storage.delete_buffer.erase(merge_mutate_entry->future_part->parts[0]->info);
                    }

                    if (!new_table_version->part_versions.insert({new_part->info, new_part_version}).second)
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Insert new inserted part version into table version failed, this is a bug, new part name: {}",
                            new_part->info.getPartName());
                    }

                    MergeTreeData::Transaction transaction(storage, merge_mutate_entry->txn.get());
                    /// FIXME Transactions: it's too optimistic, better to lock parts before starting transaction
                    storage.renameTempPartAndReplace(new_part, transaction, builder, std::move(new_table_version));
                    transaction.commit();
                }

                storage.updateMutationEntriesErrors(future_part, true, "");
                write_part_log({});

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                if (merge_mutate_entry->txn)
                    merge_mutate_entry->txn->onException();
                String exception_message = getCurrentExceptionMessage(false);
                LOG_ERROR(&Poco::Logger::get("MutatePlainUniqueMergeTreeTask"), "{}", exception_message);
                storage.updateMutationEntriesErrors(future_part, false, exception_message);
                write_part_log(ExecutionStatus::fromCurrentException());
                tryLogCurrentException(__PRETTY_FUNCTION__);
                return false;
            }
        }
        case State::NEED_FINISH :
        {
            // Nothing to do
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


}
