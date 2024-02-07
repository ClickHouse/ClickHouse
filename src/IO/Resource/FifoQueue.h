#pragma once

#include <Common/Stopwatch.h>

#include <IO/ISchedulerQueue.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <deque>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

/*
 * FIFO queue to hold pending resource requests
 */
class FifoQueue : public ISchedulerQueue
{
public:
    FifoQueue(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
        : ISchedulerQueue(event_queue_, config, config_prefix)
    {}

    bool equals(ISchedulerNode * other) override
    {
        if (auto * o = dynamic_cast<FifoQueue *>(other))
            return true;
        return false;
    }

    void enqueueRequest(ResourceRequest * request) override
    {
        std::unique_lock lock(mutex);
        request->enqueue_ns = clock_gettime_ns();
        bool was_empty = requests.empty();
        requests.push_back(request);
        if (was_empty)
            scheduleActivation();
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        std::unique_lock lock(mutex);
        if (requests.empty())
            return {nullptr, false};
        ResourceRequest * result = requests.front();
        requests.pop_front();
        return {result, !requests.empty()};
    }

    bool isActive() override
    {
        std::unique_lock lock(mutex);
        return !requests.empty();
    }

    void activateChild(ISchedulerNode *) override
    {
        assert(false); // queue cannot have children
    }

    void attachChild(const SchedulerNodePtr &) override
    {
        throw Exception(
            ErrorCodes::INVALID_SCHEDULER_NODE,
            "Cannot add child to leaf scheduler queue: {}",
            getPath());
    }

    void removeChild(ISchedulerNode *) override
    {
    }

    ISchedulerNode * getChild(const String &) override
    {
        return nullptr;
    }

private:
    std::mutex mutex;
    std::deque<ResourceRequest *> requests;
};

}
