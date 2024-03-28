#pragma once

#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>

/**
 * Implementations for transactional operations with metadata used by MetadataStorageFromDisk.
 */

namespace DB
{

class MetadataOperationsHolder
{
private:
    std::vector<MetadataOperationPtr> operations;
    MetadataFromDiskTransactionState state{MetadataFromDiskTransactionState::PREPARING};

    void rollback(size_t until_pos);

protected:
    void addOperation(MetadataOperationPtr && operation);
    void commitImpl(SharedMutex & metadata_mutex);
};

}
