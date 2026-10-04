#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/StackTraceServiceSignal.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <csignal>

namespace Poco
{
class Logger;
}

namespace DB
{

class Context;

#if defined(OS_LINUX)
class ErrnoException;

/// Whether reading a procfs file of a thread failed because the thread has exited:
/// open() fails with ENOENT or ESRCH, read() of an already opened file with ESRCH.
bool isThreadExitedError(const ErrnoException & e);
#endif

/// Allows to introspect stack trace of all server threads.
/// It acts like an embedded debugger.
/// More than one instance of this table cannot be used.
class StorageSystemStackTrace final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemStackTrace(const StorageID & table_id_);

    String getName() const override { return "SystemStackTrace"; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t /*num_streams*/) override;

    bool isSystemStorage() const override { return true; }

protected:
    LoggerPtr log;
};

}

#endif
