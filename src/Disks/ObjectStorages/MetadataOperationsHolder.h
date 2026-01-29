#pragma once

#include <mutex>
#include <vector>
#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>
#include <Common/SharedMutex.h>

/**
 * Implementations for transactional operations with metadata used by MetadataStorageFromDisk
 * and MetadataStorageFromPlainObjectStorage.
 */

namespace DB
{

class MetadataOperationsHolder
{
private:
    std::vector<MetadataOperationPtr> operations;
    MetadataStorageTransactionState state{MetadataStorageTransactionState::PREPARING};

    void rollback(std::unique_lock<SharedMutex> & lock, size_t until_pos);

protected:
    void addOperation(MetadataOperationPtr && operation);
    void commitImpl(SharedMutex & metadata_mutex);
};

}
