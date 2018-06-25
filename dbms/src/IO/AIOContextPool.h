#pragma once

#if !(defined(__FreeBSD__) || defined(__APPLE__) || defined(_MSC_VER))

#include <ext/singleton.h>
#include <condition_variable>
#include <future>
#include <mutex>
#include <map>
#include <IO/AIO.h>


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
    ID id{};
    mutable std::mutex mutex;
    mutable std::condition_variable have_resources;
    std::map<ID, std::promise<BytesRead>> promises;

    std::atomic<bool> cancelled{false};
    std::thread io_completion_monitor{&AIOContextPool::doMonitor, this};

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
