#include <cstdint>
#include <cstring>
#include <mutex>
#include <dlfcn.h>
#include <link.h>
#include <foundationdb/fdb_c.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include "FoundationDBCommon.h"
#include "FoundationDBHelpers.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int FDB_EXCEPTION;
}

FoundationDBException::FoundationDBException(fdb_error_t fdb_error)
    : Exception(fdb_get_error(fdb_error), ErrorCodes::FDB_EXCEPTION), code(fdb_error)
{
}

void throwIfFDBError(fdb_error_t error_num)
{
    if (error_num)
    {
        throw FoundationDBException(error_num);
    }
}

std::unique_ptr<ThreadFromGlobalPool> FoundationDBNetwork::network_thread;
std::mutex FoundationDBNetwork::network_thread_mutex;
std::atomic<int> FoundationDBNetwork::use_cnt = 1;

void FoundationDBNetwork::ensureStarted(int64_t thread)
{
    std::lock_guard lock(network_thread_mutex);

    if (network_thread)
    {
        return;
    }

    auto * log = &Poco::Logger::get("FoundationDBNetwork");
    throwIfFDBError(fdb_select_api_version(FDB_API_VERSION));

#ifdef FDB_ENABLE_TRACE_LOG
    std::string fdb_trace_log = "/tmp/fdb_c";
    throwIfFDBError(fdb_network_set_option(FDB_NET_OPTION_TRACE_ENABLE, FDB_VALUE_FROM_STRING(fdb_trace_log)));
    std::string fdb_trace_format = "json";
    throwIfFDBError(fdb_network_set_option(FDB_NET_OPTION_TRACE_FORMAT, FDB_VALUE_FROM_STRING(fdb_trace_format)));
#endif

    if (thread > 1)
        LOG_WARNING(log, "thread={} is not support", thread);

    throwIfFDBError(fdb_setup_network());
    network_thread = std::make_unique<ThreadFromGlobalPool>(
        [log]()
        {
            try
            {
                LOG_DEBUG(log, "Run network thread");
                throwIfFDBError(fdb_run_network());
                LOG_DEBUG(log, "Network thread stopped");
            }
            catch (...)
            {
                DB::tryLogCurrentException(log, "FDB network thread stopped unexpectedly");

                /// Since it is impossible to restart the fdb network, abort() server
                abort();
            }
        });
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

void FoundationDBNetwork::_shutdownIfNeed()
{
    release();
}

void FoundationDBNetwork::acquire()
{
    ensureStarted();
    use_cnt += 1;
}

void FoundationDBNetwork::release()
{
    if (--use_cnt > 0)
        return;

    std::lock_guard lock(network_thread_mutex);
    auto * log = &Poco::Logger::get("FoundationDBNetwork");

    if (!network_thread)
    {
        LOG_DEBUG(log, "FDB network thread was not started, ignore.");
        return;
    }

    throwIfFDBError(fdb_stop_network());
    if (network_thread->joinable())
        network_thread->join();
    network_thread.reset();
}
}
