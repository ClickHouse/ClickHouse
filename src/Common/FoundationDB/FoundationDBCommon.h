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

    /// ensureStarted() should be called before any fdb async api.
    /// It can be called multiple times, but only one network thread will be started.
    ///
    /// You can spawn multiple network threads by set thread > 1. `thread` only
    /// effective when first ensureStarted(). Multithreading should only be used
    /// during testing.
    static void ensureStarted(int64_t thread = 1);

    /// shutdownIfNeed() should be called when the program ends.
    /// It will stop the network thread if exists.
    static void shutdownIfNeed();

private:
    static std::unique_ptr<ThreadFromGlobalPool> network_thread;
    static std::mutex network_thread_mutex;
};
using FoundationDBNetworkPtr = std::shared_ptr<FoundationDBNetwork>;
}
