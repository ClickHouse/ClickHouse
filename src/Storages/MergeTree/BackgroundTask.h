#pragma once

#include <memory>

namespace DB
{

class BackgroundTask
{
public:
    virtual bool execute() = 0;

    explicit BackgroundTask(int priority_) : priority(priority_) {}

    virtual ~BackgroundTask() = default;

    bool operator> (const BackgroundTask & rhs) const
    {
        return priority > rhs.priority;
    }

    int64_t getPriority() const
    {
        return priority;
    }

    void setPriority(int64_t priority_)
    {
        priority = priority_;
    }

private:
    int64_t priority = -1;
};

using BackgroundTaskPtr = std::shared_ptr<BackgroundTask>;

}
