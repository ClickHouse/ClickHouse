#pragma once

#include <memory>

namespace DB
{

class BackgroundTask
{
public:
    virtual bool execute() = 0;
    virtual bool completedSuccessfully() = 0;
    virtual ~BackgroundTask() = default;
};

using BackgroundTaskPtr = std::shared_ptr<BackgroundTask>;

}
