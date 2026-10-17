#include <chrono>
#include <thread>
#include <gtest/gtest.h>

#include <Common/Scheduler/ISchedulerNode.h>

using namespace DB;

class FakeSchedulerNode : public ISchedulerNode
{
public:
    explicit FakeSchedulerNode(String & log_, EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
        , log(log_)
    {}

    const String & getTypeName() const override
    {
        static String type_name("fake");
        return type_name;
    }

    void attachChild(const SchedulerNodePtr & child) override
    {
        log += " +" + child->basename;
    }

    void removeChild(ISchedulerNode * child) override
    {
        log += " -" + child->basename;
    }

    ISchedulerNode * getChild(const String & /* child_name */) override
    {
        return nullptr;
    }

    void activateChild(ISchedulerNode * child) override
    {
        log += " A" + child->basename;
    }

    bool isActive() override
    {
        return false;
    }

    size_t activeChildren() override
    {
        return 0;
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        log += " D";
        return {nullptr, false};
    }

private:
    String & log;
};

struct QueueTest {
    String log;
    EventQueue event_queue;
    EventQueue::SchedulerThread scheduler_thread{&event_queue};
    FakeSchedulerNode root_node;

    QueueTest()
        : root_node(log, &event_queue)
    {}

    SchedulerNodePtr makeNode(const String & name)
    {
        auto node = std::make_shared<FakeSchedulerNode>(log, &event_queue);
        node->basename = name;
        node->setParent(&root_node);
        return std::static_pointer_cast<ISchedulerNode>(node);
    }

    void process(EventQueue::TimePoint now, const String & expected_log, size_t limit = size_t(-1))
    {
        event_queue.setManualTime(now);
        for (;limit > 0; limit--)
        {
            if (!event_queue.tryProcess())
                break;
        }
        EXPECT_EQ(log, expected_log);
        log.clear();
    }

    void activate(const SchedulerNodePtr & node)
    {
        event_queue.enqueueActivation(node.get());
    }

    void event(const String & text)
    {
        event_queue.enqueue([this, text] { log += " " + text; });
    }

    EventId postpone(EventQueue::TimePoint until, const String & text)
    {
        return event_queue.postpone(until, [this, text] { log += " " + text; });
    }

    void cancel(EventId event_id)
    {
        event_queue.cancelPostponed(event_id);
    }
};

TEST(SchedulerEventQueue, Smoke)
{
    QueueTest t;

    using namespace std::chrono_literals;

    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, "", 0);

    // Activations
    auto node1 = t.makeNode("1");
    auto node2 = t.makeNode("2");
    t.activate(node2);
    t.activate(node1);
    t.process(start + 42s, " A2 A1");

    // Events
    t.event("E1");
    t.event("E2");
    t.process(start + 100s, " E1 E2");

    // Postponed events
    t.postpone(start + 200s, "P200");
    auto p190 = t.postpone(start + 200s, "P190");
    t.postpone(start + 150s, "P150");
    t.postpone(start + 175s, "P175");
    t.process(start + 180s, " P150 P175");
    t.event("E3");
    t.cancel(p190);
    t.process(start + 300s, " E3 P200");

    // Ordering of events and activations
    t.event("E1");
    t.activate(node1);
    t.event("E2");
    t.activate(node2);
    t.process(start + 300s, " E1 A1 E2 A2");
}

// Test for data race between processActivation() and cancelActivation() during node destruction.
TEST(SchedulerEventQueue, ActivationCancelRace)
{
    constexpr int iterations = 1000;

    for (int i = 0; i < iterations; ++i)
    {
        String log;
        EventQueue event_queue;

        std::atomic<bool> race_detected{false};
        std::atomic<bool> child_active{false};
        std::atomic<bool> in_activate_child{false};

        // Parent node that simulates the race scenario
        struct RaceDetectorNode : public FakeSchedulerNode
        {
            std::atomic<bool> & race;
            std::atomic<bool> & active;
            std::atomic<bool> & in_activate;

            RaceDetectorNode(String & log_, EventQueue * eq,
                             std::atomic<bool> & r, std::atomic<bool> & a, std::atomic<bool> & ia)
                : FakeSchedulerNode(log_, eq), race(r), active(a), in_activate(ia) {}

            void activateChild(ISchedulerNode *) override
            {
                in_activate.store(true, std::memory_order_release);
                // Simulate some work to widen the race window
                std::this_thread::yield();
                active.store(true, std::memory_order_relaxed); // Like UnifiedSchedulerNode::activateChild
                in_activate.store(false, std::memory_order_release);
            }
        };

        RaceDetectorNode root_node(log, &event_queue, race_detected, child_active, in_activate_child);

        std::atomic<bool> stop{false};

        // Scheduler thread - processes activations
        std::thread processor([&]
        {
            EventQueue::SchedulerThread scheduler_thread(&event_queue);
            while (!stop.load(std::memory_order_relaxed))
                event_queue.tryProcess();
            while (event_queue.tryProcess()) {}
        });

        // Create nodes and simulate the race scenario
        for (int j = 0; j < 50; ++j)
        {
            child_active.store(false, std::memory_order_relaxed);

            auto node = std::make_shared<FakeSchedulerNode>(log, &event_queue);
            node->basename = std::to_string(j);
            node->setParent(&root_node);
            event_queue.enqueueActivation(node.get());

            // Simulate what a correct removeChild should do:
            // 1. First cancel activation and wait for any in-progress activateChild
            event_queue.cancelActivation(node.get());

            // 2. Now check if activateChild is still running - it shouldn't be!
            if (in_activate_child.load(std::memory_order_acquire))
                race_detected.store(true, std::memory_order_release);

            // 3. Now safe to modify child_active (like removeChild does)
            child_active.store(false, std::memory_order_relaxed);

            node->setParent(nullptr); // detach (calls cancelActivation again, which is idempotent)
        }

        stop.store(true, std::memory_order_relaxed);
        processor.join();

        // With the fix: after cancelActivation() returns, activateChild() is guaranteed
        // to have completed, so in_activate_child should never be true at that point
        EXPECT_FALSE(race_detected.load())
            << "activateChild() was still in progress after cancelActivation() returned";
    }
}
