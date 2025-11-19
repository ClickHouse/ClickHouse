#pragma once

#include <Disks/DiskCommitTransactionOptions.h>
#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>

#include <vector>

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
    void addOperation(MetadataOperationPtr && operation);
    void commit();
    void finalize() noexcept;

private:
    std::vector<MetadataOperationPtr> operations;
    MetadataStorageTransactionState state{MetadataStorageTransactionState::PREPARING};
};

}
