#pragma once

#include <memory>

namespace DB
{

struct IMetadataOperation
{
    virtual void execute() = 0;
    virtual void undo() {}
    virtual void finalize() {}
    virtual ~IMetadataOperation() = default;
};

using MetadataOperationPtr = std::unique_ptr<IMetadataOperation>;

}
