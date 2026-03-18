#pragma once

#include <Common/Stopwatch.h>

#include <Common/Scheduler/ISchedulerQueue.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/intrusive/list.hpp>

#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SCHEDULER_NODE;
}

/*
 * FIFO queue to hold pending resource requests
 */
class FifoQueue final : public ISchedulerQueue
{
public:
    FifoQueue(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
        : ISchedulerQueue(event_queue_, config, config_prefix)
    {}

    FifoQueue(EventQueue * event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerQueue(event_queue_, info_)
    {}

    ~FifoQueue() override
    {
        purgeQueue();
    }

    const String & getTypeName() const override
    {
        static String type_name("fifo");
        return type_name;
    }

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * _ = dynamic_cast<FifoQueue *>(other))
            return true;
        return false;
    }

    void enqueueRequest(ResourceRequest * request) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue is about to be destructed");
        queue_cost += request->cost;
        bool was_empty = requests.empty();
        requests.push_back(*request);
        if (was_empty)
            scheduleActivation();
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        std::lock_guard lock(mutex);
        if (requests.empty())
            return {nullptr, false};
        ResourceRequest * result = &requests.front();
        requests.pop_front();
        if (requests.empty())
            busy_periods++;
        queue_cost -= result->cost;
        incrementDequeued(result->cost);
        return {result, !requests.empty()};
    }

    bool cancelRequest(ResourceRequest * request) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            return false; // Any request should already be failed or executed
        if (request->is_linked())
        {
            // It's impossible to check that `request` is indeed inserted to this queue and not another queue.
            // It's up to caller to make sure this is the case. Otherwise, list sizes will be corrupted.
            // Not tracking list sizes is not an option, because another problem appears: removing from list w/o locking.
            // Another possible solution - keep track if request `is_cancelable` guarded by `mutex`
            // Simple check for list size corruption
            if (requests.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "trying to cancel request (linked into another queue) from empty queue: {}", getPath());

            requests.erase(requests.iterator_to(*request));

            if (requests.empty())
                busy_periods++;
            queue_cost -= request->cost;
            canceled_requests++;
            canceled_cost += request->cost;
            return true;
        }
        return false;
    }

    void purgeQueue() override
    {
        std::lock_guard lock(mutex);
        is_not_usable = true;
        while (!requests.empty())
        {
            ResourceRequest * request = &requests.front();
            requests.pop_front();
            request->failed(std::make_exception_ptr(
                Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue with resource request is about to be destructed")));
        }
    }

    bool isActive() override
    {
        std::lock_guard lock(mutex);
        return !requests.empty();
    }

    size_t activeChildren() override
    {
        return 0;
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

    std::pair<UInt64, Int64> getQueueLengthAndCost()
    {
        std::lock_guard lock(mutex);
        return {requests.size(), queue_cost};
    }

private:
    std::mutex mutex;
    Int64 queue_cost = 0;
    boost::intrusive::list<ResourceRequest> requests;
    bool is_not_usable = false;
};

}
