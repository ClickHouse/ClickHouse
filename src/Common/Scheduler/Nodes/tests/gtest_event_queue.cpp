#include <chrono>
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
