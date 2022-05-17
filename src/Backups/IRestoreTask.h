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

    enum class RestoreKind
    {
        /// This task restores metadata (definitions of databases and tables).
        /// Tasks restoring metadata are executed first and strictly in one thread.
        METADATA,

        /// This task restores tables' data. Such tasks can be executed in parallel.
        DATA,
    };

    virtual RestoreKind getRestoreKind() const { return RestoreKind::DATA; }

    /// Perform restoring, the function also can return a list of nested tasks that should be run later.
    virtual std::vector<std::unique_ptr<IRestoreTask>> run() = 0;
};

using RestoreTaskPtr = std::unique_ptr<IRestoreTask>;
using RestoreTasks = std::vector<RestoreTaskPtr>;

}
