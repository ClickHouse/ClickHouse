#include <cstdint>
#include <cstring>
#include <mutex>
#include <dlfcn.h>
#include <link.h>
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>
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
    {
#ifdef FDB_C_EMBED
        LOG_WARNING(log, "thread={} is ignored due to fdb_c is embeded", thread);
#else
        void * sym_ptr = dlsym(RTLD_DEFAULT, "fdb_setup_network");
        Dl_info info;
        int res = 0;

        if (sym_ptr)
            res = dladdr(sym_ptr, &info);

        if (res == 0)
        {
            LOG_WARNING(log, "thread is ignored due to shared object libfdb_c is not found");
        }
        else
        {
            LOG_DEBUG(log, "using external fdb_c: {}", info.dli_fname);
            throwIfFDBError(fdb_network_set_option(
                FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY, reinterpret_cast<const uint8_t *>(info.dli_fname), std::strlen(info.dli_fname)));
            throwIfFDBError(fdb_network_set_option(FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION, FDB_VALUE_FROM_POD(thread)));
        }
#endif
    }

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

void FoundationDBNetwork::shutdownIfNeed()
{
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
