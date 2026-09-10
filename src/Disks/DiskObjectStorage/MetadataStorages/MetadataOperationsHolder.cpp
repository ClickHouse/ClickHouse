#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <exception>

namespace ProfileEvents
{
    extern const Event MetadataTransactionRollbacks;
    extern const Event MetadataTransactionRollbacksFailed;
}

namespace DB
{

namespace ErrorCodes
{
extern const int FS_METADATA_ERROR;
}

void MetadataOperationsHolder::rollback(size_t until_pos, Exception & rollback_reason) noexcept
{
    ProfileEvents::increment(ProfileEvents::MetadataTransactionRollbacks);

    for (int64_t i = until_pos; i >= 0; --i)
    {
        try
        {
            operations[i]->undo();
        }
        catch (...)
        {
            /// The operations below this one keep whatever they have written, so object storage may now describe a
            /// filesystem this process does not have, until the filesystem is next loaded from object storage.
            ProfileEvents::increment(ProfileEvents::MetadataTransactionRollbacksFailed);

            state = MetadataStorageTransactionState::PARTIALLY_ROLLED_BACK;

            rollback_reason.addMessage(fmt::format("While rolling back operation #{}", i));
            rollback_reason.addMessage(getExceptionMessage(std::current_exception(), /*with_stacktrace=*/true));

            return;
        }
    }
}

void MetadataOperationsHolder::prependOperation(MetadataOperationPtr && operation)
{
    if (state != MetadataStorageTransactionState::PREPARING)
        throw Exception(
            ErrorCodes::FS_METADATA_ERROR,
            "Cannot add operations to transaction in {} state, it should be in {} state",
            toString(state),
            toString(MetadataStorageTransactionState::PREPARING));

    operations.emplace_front(std::move(operation));
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

void MetadataOperationsHolder::commit()
{
    if (state != MetadataStorageTransactionState::PREPARING)
        throw Exception(
            ErrorCodes::FS_METADATA_ERROR,
            "Cannot commit transaction in {} state, it should be in {} state",
            toString(state),
            toString(MetadataStorageTransactionState::PREPARING));

    for (size_t i = 0; i < operations.size(); ++i)
    {
        try
        {
            operations[i]->execute();
        }
        catch (Exception & error)
        {
            state = MetadataStorageTransactionState::FAILED;

            error.addMessage(fmt::format("While committing metadata operation #{}", i));
            rollback(i, error);

            tryLogCurrentException(__PRETTY_FUNCTION__);
            error.rethrow();
        }
    }

    state = MetadataStorageTransactionState::COMMITTED;
}

void MetadataOperationsHolder::finalize() noexcept
{
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
}

}
