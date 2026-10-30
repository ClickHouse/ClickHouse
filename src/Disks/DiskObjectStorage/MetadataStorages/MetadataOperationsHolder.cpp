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

std::optional<String> MetadataOperationsHolder::rollback(size_t until_pos) noexcept
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
            ProfileEvents::increment(ProfileEvents::MetadataTransactionRollbacksFailed);

            state = MetadataStorageTransactionState::PARTIALLY_ROLLED_BACK;

            return fmt::format(
                "While rolling back operation #{}: {}: Rolling back the metadata transaction did not complete, so the "
                "metadata keeps a part of a transaction that is reported as failed",
                i,
                getExceptionMessage(std::current_exception(), /*with_stacktrace=*/true));
        }
    }

    return {};
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
        catch (...)
        {
            state = MetadataStorageTransactionState::FAILED;

            String details = fmt::format("While committing metadata operation #{}", i);
            const auto rollback_failure = rollback(i);
            if (rollback_failure)
                details += ": " + *rollback_failure;

            /// The original exception is rethrown as is, so the callers that check its type still can. Some object storages
            /// (Azure) throw their own exception types, which cannot take the details; those are rethrown as is only when the
            /// rollback completed.
            if (auto * error = current_exception_cast<Exception *>())
            {
                error->addMessage(details);
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
            else if (rollback_failure)
            {
                /// A partial rollback has to reach the caller, and a foreign exception cannot carry it.
                Exception report(getCurrentExceptionMessageAndPattern(/*with_stacktrace=*/ true), getCurrentExceptionCode());
                report.addMessage(details);
                tryLogException(std::make_exception_ptr(report), __PRETTY_FUNCTION__);
                report.rethrow();
            }
            else
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, details);
            }
            throw;
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
