#pragma once

/// Classes required to use FDB-based features like MetadataStore and FDBKeeper.

#include <memory>
#include <mutex>
#include <config.h>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include "fdb_c_fwd.h"

namespace DB
{
class FoundationDBException : public Exception
{
public:
    explicit FoundationDBException(fdb_error_t fdb_error);
    const fdb_error_t code;
};

void throwIfFDBError(fdb_error_t error_num);

struct FoundationDBOptions
{
    FoundationDBOptions() = default;
    FoundationDBOptions(const Poco::Util::AbstractConfiguration & config, const String & config_name);

    std::vector<String> knobs;
};

/// The FoundationDB client library performs most tasks on a singleton network thread.
/// Because of the limitation of fdb library, at most one network thread can be started.
/// After the network thread stops, it cannot be started again.
class FoundationDBNetwork : public boost::noncopyable
{
public:
    FoundationDBNetwork() = delete;

    /// ensureStarted() should be called before any fdb async api.
    /// It can be called multiple times, but it only has effect the first time
    /// you call it.
    static void ensureStarted(FoundationDBOptions options = {});

    /// shutdownIfNeed() should be called when the program ends.
    /// It will stop the network thread if exists.
    /// If holders exists, the network thread will be stopped after all holders
    /// have been released.
    static void shutdownIfNeedImpl();

    static void shutdownIfNeed()
    {
#if USE_FDB
        shutdownIfNeedImpl();
#endif
    }
    /// Holder ensures that the FoundationDB Network thread remains active,
    /// even after shutdownIfNeed(). Holder implicitly ensureStarted(), so you
    /// don't need to call ensureStarted() if you are using Holder.
    struct Holder
    {
        Holder() { acquire(); }
        ~Holder() { release(); }
    };

private:
    static std::unique_ptr<ThreadFromGlobalPool> network_thread;
    static std::mutex network_thread_mutex;
    static std::atomic<int> use_cnt;

    static void acquire();
    static void release();
};
}
