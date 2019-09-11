#pragma once

#if defined(__linux__) || defined(__FreeBSD__)

#include <ext/singleton.h>
#include <condition_variable>
#include <future>
#include <mutex>
#include <map>
#include <IO/AIO.h>
#include <Common/ThreadPool.h>


namespace DB
{

class AIOContextPool : public ext::singleton<AIOContextPool>
{
    friend class ext::singleton<AIOContextPool>;

    static const auto max_concurrent_events = 128;
    static const auto timeout_sec = 1;

    AIOContext aio_context{max_concurrent_events};

    using ID = size_t;
    using BytesRead = ssize_t;

    /// Autoincremental id used to identify completed requests
    ID next_id{};
    mutable std::mutex mutex;
    mutable std::condition_variable have_resources;
    std::map<ID, std::promise<BytesRead>> promises;

    std::atomic<bool> cancelled{false};
    ThreadFromGlobalPool io_completion_monitor{&AIOContextPool::doMonitor, this};

    ~AIOContextPool();

    void doMonitor();
    void waitForCompletion();
    int getCompletionEvents(io_event events[], const int max_events);
    void fulfillPromises(const io_event events[], const int num_events);
    void notifyProducers(const int num_producers) const;
    void reportExceptionToAnyProducer();

public:
    /// Request AIO read operation for iocb, returns a future with number of bytes read
    std::future<BytesRead> post(struct iocb & iocb);
};

}

#endif
