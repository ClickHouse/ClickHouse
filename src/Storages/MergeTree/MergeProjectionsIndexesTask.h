#pragma once
#include <Storages/MergeTree/IExecutableTask.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

class IMergeTreeDataPart;
struct MergeTreeDataPartChecksums;

using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using MutableDataPartsVector = std::vector<MutableDataPartPtr>;

class MergeProjectionsIndexesTask : public IExecutableTask
{
public:
    virtual MutableDataPartsVector extractTemporaryParts() = 0;
    virtual void addToChecksums(MergeTreeDataPartChecksums & checksums) = 0;

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
};

}
