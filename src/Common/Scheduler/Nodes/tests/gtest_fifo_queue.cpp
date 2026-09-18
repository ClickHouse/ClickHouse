#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/Nodes/TimeShared/FifoQueue.h>
#include <Common/Scheduler/ResourceRequest.h>

#include <gtest/gtest.h>

#include <functional>

namespace DB
{
namespace
{
class CallbackRequest : public ResourceRequest
{
public:
    explicit CallbackRequest(ResourceCost cost_) : ResourceRequest(cost_)
    {}

    void execute() override
    {
        ADD_FAILURE() << "Unexpected request execution";
    }

    void failed(const std::exception_ptr & exception) override
    {
        EXPECT_NE(exception, nullptr);
        ++failures;
        if (on_failure)
            on_failure();
    }

    size_t failures = 0;
    std::function<void()> on_failure;
};
}

TEST(FifoQueue, LimitFailureCanCancelRemainingRequest)
{
    EventQueue events;
    CallbackRequest first(3);
    CallbackRequest second(7);
    FifoQueue queue(events);
    second.on_failure = [&]
    {
        const auto [length, cost] = queue.getQueueLengthAndCost();
        EXPECT_EQ(length, 1);
        EXPECT_EQ(cost, 3);
        EXPECT_EQ(queue.rejected_requests.load(), 1);
        EXPECT_EQ(queue.rejected_cost.load(), 7);
        EXPECT_TRUE(queue.cancelRequest(&first));
        EXPECT_EQ(queue.busy_periods.load(), 1);
    };

    queue.enqueueRequest(&first);
    queue.enqueueRequest(&second);
    queue.updateQueueLimit(1);

    EXPECT_EQ(first.failures, 0);
    EXPECT_EQ(second.failures, 1);
    EXPECT_EQ(queue.canceled_requests.load(), 1);
    EXPECT_EQ(queue.canceled_cost.load(), 3);
    EXPECT_EQ(queue.busy_periods.load(), 1);
    EXPECT_EQ(queue.getQueueLengthAndCost().first, 0);
    EXPECT_EQ(queue.getQueueLengthAndCost().second, 0);
}

TEST(FifoQueue, LimitShrinkAccountsBeforeCallback)
{
    EventQueue events;
    CallbackRequest first(3);
    CallbackRequest second(5);
    CallbackRequest third(7);
    FifoQueue queue(events);
    third.on_failure = [&]
    {
        const auto [length, cost] = queue.getQueueLengthAndCost();
        EXPECT_EQ(length, 2);
        EXPECT_EQ(cost, 8);
        EXPECT_EQ(queue.rejected_requests.load(), 1);
        EXPECT_EQ(queue.rejected_cost.load(), 7);
        EXPECT_EQ(queue.busy_periods.load(), 0);
    };

    queue.enqueueRequest(&first);
    queue.enqueueRequest(&second);
    queue.enqueueRequest(&third);
    queue.updateQueueLimit(2);

    EXPECT_EQ(first.failures, 0);
    EXPECT_EQ(second.failures, 0);
    EXPECT_EQ(third.failures, 1);
    EXPECT_EQ(queue.busy_periods.load(), 0);
    EXPECT_TRUE(queue.cancelRequest(&first));
    EXPECT_TRUE(queue.cancelRequest(&second));
}
}
