#pragma once

#include <memory>
#include <vector>


namespace DB
{

/// Represents a task of restoring something (database / table / table's part) from backup.
class IRestoreFromBackupTask
{
public:
    IRestoreFromBackupTask() = default;
    virtual ~IRestoreFromBackupTask() = default;

    /// Perform restoring, the function also can return a list of nested tasks that should be run later.
    virtual std::vector<std::unique_ptr<IRestoreFromBackupTask>> run() = 0;

    /// Is it necessary to run this task sequentially?
    /// Sequential tasks are executed first and strictly in one thread.
    virtual bool isSequential() const { return false; }

    /// Reverts the effect of run(). If that's not possible, the function does nothing.
    virtual void rollback() {}
};

using RestoreFromBackupTaskPtr = std::unique_ptr<IRestoreFromBackupTask>;
using RestoreFromBackupTasks = std::vector<RestoreFromBackupTaskPtr>;

}
