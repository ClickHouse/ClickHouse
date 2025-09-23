#include <Disks/ObjectStorages/MetadataOperationsHolder.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FS_METADATA_ERROR;
}

void MetadataOperationsHolder::rollback(std::unique_lock<SharedMutex> & lock, size_t until_pos)
{
    /// Otherwise everything is alright
    if (state == MetadataStorageTransactionState::FAILED)
    {
        for (int64_t i = until_pos; i >= 0; --i)
        {
            try
            {
                operations[i]->undo(lock);
            }
            catch (Exception & ex)
            {
                state = MetadataStorageTransactionState::PARTIALLY_ROLLED_BACK;
                ex.addMessage(fmt::format("While rolling back operation #{}", i));
                throw;
            }
        }
    }
    else
    {
        /// Nothing to do, transaction committed or not even started to commit
    }
}

void MetadataOperationsHolder::addOperation(MetadataOperationPtr && operation)
{
    if (state != MetadataStorageTransactionState::PREPARING)
        throw Exception(
            ErrorCodes::FS_METADATA_ERROR,
            "Cannot add operations to transaction in {} state, it should be in {} state",
            toString(state),
            toString(MetadataStorageTransactionState::PREPARING));

    operations.emplace_back(std::move(operation));
}

void MetadataOperationsHolder::commitImpl(SharedMutex & metadata_mutex)
{
    if (state != MetadataStorageTransactionState::PREPARING)
        throw Exception(
            ErrorCodes::FS_METADATA_ERROR,
            "Cannot commit transaction in {} state, it should be in {} state",
            toString(state),
            toString(MetadataStorageTransactionState::PREPARING));

    {
        std::unique_lock lock(metadata_mutex);
        for (size_t i = 0; i < operations.size(); ++i)
        {
            try
            {
                operations[i]->execute(lock);
            }
            catch (Exception & ex)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                ex.addMessage(fmt::format("While committing metadata operation #{}", i));
                state = MetadataStorageTransactionState::FAILED;
                rollback(lock, i);
                throw;
            }
        }
    }

    /// Do it in "best effort" mode
    for (size_t i = 0; i < operations.size(); ++i)
    {
        try
        {
            operations[i]->finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Failed to finalize operation #{}", i));
        }
    }

    state = MetadataStorageTransactionState::COMMITTED;
}
}
