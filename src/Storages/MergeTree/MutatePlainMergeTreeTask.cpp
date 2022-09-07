#include <Storages/MergeTree/MutatePlainMergeTreeTask.h>

#include <Storages/StorageMergeTree.h>
#include <Interpreters/TransactionLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageID MutatePlainMergeTreeTask::getStorageID()
{
    return storage.getStorageID();
}

void MutatePlainMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}


void MutatePlainMergeTreeTask::prepare()
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

bool MutatePlainMergeTreeTask::executeStep()
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


                MergeTreeData::Transaction transaction(storage, merge_mutate_entry->txn.get());
                /// FIXME Transactions: it's too optimistic, better to lock parts before starting transaction
                storage.renameTempPartAndReplace(new_part, transaction, builder);
                transaction.commit();

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
                LOG_ERROR(&Poco::Logger::get("MutatePlainMergeTreeTask"), "{}", exception_message);
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
