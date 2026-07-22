#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

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
const int STACK_TRACE_SERVICE_SIGNAL = SIGRTMIN;
#elif defined(OS_DARWIN)
/// macOS has no real-time signals; SIGUSR1/SIGUSR2 are the query profiler's, so use the free
/// virtual-timer signal here.
const int STACK_TRACE_SERVICE_SIGNAL = SIGVTALRM;
#else
#error "STACK_TRACE_SERVICE_SIGNAL is not defined for this platform"
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
