#pragma once

#include <memory>

#include <Interpreters/StorageID.h>

namespace DB
{

class ExecutableTask
{
public:
    virtual bool execute() = 0;
    virtual void onCompleted() = 0;
    virtual StorageID getStorageID() = 0;
    virtual ~ExecutableTask() = default;
};

using ExecutableTaskPtr = std::shared_ptr<ExecutableTask>;

}
