#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

namespace DB
{

enum class MetadataFromDiskTransactionState
{
    PREPARING,
    FAILED,
    COMMITTED,
    PARTIALLY_ROLLED_BACK,
};

std::string toString(MetadataFromDiskTransactionState state);

struct IMetadataOperation
{
    virtual void execute() = 0;
    virtual void undo() = 0;
    virtual void finalize() {}
    virtual ~IMetadataOperation() = default;
};

using MetadataOperationPtr = std::unique_ptr<IMetadataOperation>;

/**
 *                                                               -> MetadataStorageFromRemoteDiskTransaction
 * IMetadataTransaction -> MetadataStorageFromDiskTransaction  |
 *                                                               -> MetadataStorageFromLocalDiskTransaction
 */
class MetadataStorageFromDiskTransaction : public IMetadataTransaction
{
public:
    explicit MetadataStorageFromDiskTransaction(const IMetadataStorage & metadata_storage_);

    ~MetadataStorageFromDiskTransaction() override = default;

    const IMetadataStorage & getStorageForNonTransactionalReads() const final;

    void commit() final;


protected:
    void addOperation(MetadataOperationPtr && operation);

    void rollback(size_t until_pos);

    const IMetadataStorage & metadata_storage;
    std::vector<MetadataOperationPtr> operations;
    MetadataFromDiskTransactionState state{MetadataFromDiskTransactionState::PREPARING};
};

}
