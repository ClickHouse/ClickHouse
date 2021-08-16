#pragma once

#include <memory>

#include <Interpreters/StorageID.h>

namespace DB
{

class BackgroundTask
{
public:
    virtual bool execute() = 0;
    virtual void onCompleted() = 0;
    virtual StorageID getStorageID() = 0;
    virtual ~BackgroundTask() = default;
};

using BackgroundTaskPtr = std::shared_ptr<BackgroundTask>;

}
