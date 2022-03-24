#pragma once

#include <memory>
#include <vector>


namespace DB
{

/// Represents a task of restoring something (database / table / table's part) from backup.
class IRestoreTask
{
public:
    IRestoreTask() = default;
    virtual ~IRestoreTask() = default;

    /// Perform restoring, the function also can return a list of nested tasks that should be run later.
    virtual std::vector<std::unique_ptr<IRestoreTask>> run() = 0;

    /// Is it necessary to run this task sequentially?
    /// Sequential tasks are executed first and strictly in one thread.
    virtual bool isSequential() const { return false; }

    /// Reverts the effect of run(). If that's not possible, the function does nothing.
    virtual void rollback() {}
};

using RestoreTaskPtr = std::unique_ptr<IRestoreTask>;
using RestoreTasks = std::vector<RestoreTaskPtr>;

}
