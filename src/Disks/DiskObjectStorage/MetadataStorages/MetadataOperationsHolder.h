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
    void prependOperation(MetadataOperationPtr && operation);
    void addOperation(MetadataOperationPtr && operation);
    void commit();
    void finalize() noexcept;

private:
    std::deque<MetadataOperationPtr> operations;
    MetadataStorageTransactionState state{MetadataStorageTransactionState::PREPARING};
};

}
