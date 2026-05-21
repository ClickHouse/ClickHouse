#pragma once

#include <mutex>
#include <Common/SharedMutex.h>

namespace DB
{

struct IMetadataOperation
{
    virtual void execute(std::unique_lock<SharedMutex> & metadata_lock) = 0;
    virtual void undo(std::unique_lock<SharedMutex> & metadata_lock) = 0;
    virtual void finalize() { }
    virtual ~IMetadataOperation() = default;
};

using MetadataOperationPtr = std::unique_ptr<IMetadataOperation>;

}
