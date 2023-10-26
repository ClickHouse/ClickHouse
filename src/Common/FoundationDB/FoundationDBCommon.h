#pragma once

/// Classes required to use FDB-based features like MetadataStore and FDBKeeper.

#include <memory>
#include <mutex>
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

/// The FoundationDB client library performs most tasks on a singleton network thread.
/// Because of the limitation of fdb library, at most one network thread can be started.
/// After the network thread stops, it cannot be started again.
class FoundationDBNetwork : public boost::noncopyable
{
public:
    FoundationDBNetwork() = delete;

    /// Set the target path for libfdb_c.so. Only valid before ensureStarted().
    static void setLibraryPath(const std::string & path);

    /// ensureStarted() should be called before any fdb async api.
    /// It can be called multiple times, but only one network thread will be started.
    ///
    /// You can spawn multiple network threads by set thread > 1. `thread` only
    /// effective when first ensureStarted(). Multithreading should only be used
    /// during testing.
    static void ensureStarted(int64_t thread = 1);

    /// shutdownIfNeed() should be called when the program ends.
    /// It will stop the network thread if exists.
    /// If holders exists, the network thread will be stopped after all holders
    /// have been released.
    static void _shutdownIfNeed();

    static void shutdownIfNeed()
    {
#ifdef ENABLE_FDB
        _shutdownIfNeed();
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
