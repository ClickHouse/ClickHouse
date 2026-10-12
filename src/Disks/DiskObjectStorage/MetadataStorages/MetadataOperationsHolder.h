#pragma once

#include <Disks/DiskCommitTransactionOptions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataOperation.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>

#include <deque>

namespace DB
{

/**
 * Implementations for transactional operations with metadata used by
 * 1. MetadataStorageFromDisk
 * 2. MetadataStorageFromPlainObjectStorage.
 */
class MetadataOperationsHolder
{
    void rollback(size_t until_pos, Exception & rollback_reason) noexcept;

public:
    /// With `noexcept_rollback_`, an `undo` that throws terminates the server instead of leaving the transaction partially rolled back.
    explicit MetadataOperationsHolder(bool noexcept_rollback_ = false) : noexcept_rollback(noexcept_rollback_) {}

    void prependOperation(MetadataOperationPtr && operation);
    void addOperation(MetadataOperationPtr && operation);
    void commit();
    void finalize() noexcept;

private:
    const bool noexcept_rollback;
    std::deque<MetadataOperationPtr> operations;
    MetadataStorageTransactionState state{MetadataStorageTransactionState::PREPARING};
};

}
