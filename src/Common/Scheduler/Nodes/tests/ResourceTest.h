#pragma once

#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/SchedulerRoot.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Scheduler/Nodes/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/FifoQueue.h>
#include <Common/Scheduler/Nodes/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>
#include <Common/Scheduler/Nodes/registerResourceManagers.h>

#include <Poco/Util/XMLConfiguration.h>

#include <atomic>
#include <barrier>
#include <unordered_map>
#include <mutex>
#include <set>
#include <sstream>

namespace DB
{

struct ResourceTestBase
{
    ResourceTestBase()
    {
        [[maybe_unused]] static bool typesRegistered = [] { registerSchedulerNodes(); registerResourceManagers(); return true; }();
    }

    template <class TClass>
    static TClass * add(EventQueue * event_queue, SchedulerNodePtr & root_node, const String & path, const String & xml = {})
    {
        std::stringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        stream << "<resource><node path=\"" << path << "\">" << xml << "</node></resource>";
        Poco::AutoPtr config{new Poco::Util::XMLConfiguration(stream)};
        String config_prefix = "node";

        if (path == "/")
        {
            EXPECT_TRUE(root_node.get() == nullptr);
            root_node.reset(new TClass(event_queue, *config, config_prefix));
            return static_cast<TClass *>(root_node.get());
        }

        EXPECT_TRUE(root_node.get() != nullptr); // root should be initialized first
        ISchedulerNode * parent = root_node.get();
        size_t pos = 1;
        String child_name;
        while (pos < path.length())
        {
            size_t slash = path.find('/', pos);
            if (slash != String::npos)
            {
                parent = parent->getChild(path.substr(pos, slash - pos));
                EXPECT_TRUE(parent != nullptr); // parent does not exist
                pos = slash + 1;
            }
            else
            {
                child_name = path.substr(pos);
                pos = String::npos;
            }
        }

        EXPECT_TRUE(!child_name.empty()); // wrong path
        SchedulerNodePtr node = std::make_shared<TClass>(event_queue, *config, config_prefix);
        node->basename = child_name;
        parent->attachChild(node);
        return static_cast<TClass *>(node.get());
    }
};


struct ConstraintTest : public SemaphoreConstraint
{
    explicit ConstraintTest(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : SemaphoreConstraint(event_queue_, config, config_prefix)
    {}

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        auto [request, active] = SemaphoreConstraint::dequeueRequest();
        if (request)
        {
            std::unique_lock lock(mutex);
            requests.insert(request);
        }
        return {request, active};
    }

    void finishRequest(ResourceRequest * request) override
    {
        {
            std::unique_lock lock(mutex);
            requests.erase(request);
        }
        SemaphoreConstraint::finishRequest(request);
    }

    std::mutex mutex;
    std::set<ResourceRequest *> requests;
};

class ResourceTestClass : public ResourceTestBase
{
    struct Request : public ResourceRequest
    {
        String name;

        Request(ResourceCost cost_, const String & name_)
            : ResourceRequest(cost_)
            , name(name_)
        {}

        void execute() override
        {
        }
    };

public:
    template <class TClass>
    void add(const String & path, const String & xml = {})
    {
        ResourceTestBase::add<TClass>(&event_queue, root_node, path, xml);
    }

    void enqueue(const String & path, const std::vector<ResourceCost> & costs)
    {
        ASSERT_TRUE(root_node.get() != nullptr); // root should be initialized first
        ISchedulerNode * node = root_node.get();
        size_t pos = 1;
        while (pos < path.length())
        {
            size_t slash = path.find('/', pos);
            if (slash != String::npos)
            {
                node = node->getChild(path.substr(pos, slash - pos));
                ASSERT_TRUE(node != nullptr); // does not exist
                pos = slash + 1;
            }
            else
            {
                node = node->getChild(path.substr(pos));
                pos = String::npos;
            }
        }
        ISchedulerQueue * queue = dynamic_cast<ISchedulerQueue *>(node);
        ASSERT_TRUE(queue != nullptr); // not a queue

        for (ResourceCost cost : costs)
        {
            queue->enqueueRequest(new Request(cost, queue->basename));
        }
        processEvents(); // to activate queues
    }

    void dequeue(size_t count_limit = size_t(-1), ResourceCost cost_limit = ResourceCostMax)
    {
        while (count_limit > 0 && cost_limit > 0)
        {
            if (auto [request, _] = root_node->dequeueRequest(); request)
            {
                count_limit--;
                cost_limit -= request->cost;
                handle(static_cast<Request *>(request));
            }
            else
            {
                break;
            }
        }
    }

    void process(EventQueue::TimePoint now, size_t count_limit = size_t(-1), ResourceCost cost_limit = ResourceCostMax)
    {
        event_queue.setManualTime(now);

        while (count_limit > 0 && cost_limit > 0)
        {
            processEvents();
            if (!root_node->isActive())
                return;
            if (auto [request, _] = root_node->dequeueRequest(); request)
            {
                count_limit--;
                cost_limit -= request->cost;
                handle(static_cast<Request *>(request));
            }
            else
            {
                break;
            }
        }
    }

    void handle(Request * request)
    {
        consumed_cost[request->name] += request->cost;
        delete request;
    }

    void consumed(const String & name, ResourceCost value, ResourceCost error = 0)
    {
        EXPECT_GE(consumed_cost[name], value - error);
        EXPECT_LE(consumed_cost[name], value + error);
        consumed_cost[name] -= value;
    }

    void processEvents()
    {
        while (event_queue.tryProcess()) {}
    }

private:
    EventQueue event_queue;
    SchedulerNodePtr root_node;
    std::unordered_map<String, ResourceCost> consumed_cost;
};

template <class TManager>
struct ResourceTestManager : public ResourceTestBase
{
    ResourceManagerPtr manager;

    std::vector<ThreadFromGlobalPool> threads;
    std::barrier<> busy_period;

    struct Guard : public ResourceGuard
    {
        ResourceTestManager & t;

        Guard(ResourceTestManager & t_, ResourceLink link_, ResourceCost cost)
            : ResourceGuard(ResourceGuard::Metrics::getIOWrite(), link_, cost, Lock::Defer)
            , t(t_)
        {
            t.onEnqueue(link);
            lock();
            t.onExecute(link);
            consume(cost);
        }
    };

    struct TItem
    {
        std::atomic<Int64> enqueued = 0; // number of enqueued requests
        std::atomic<Int64> left = 0; // number of requests left to be executed
    };

    struct ResourceQueueHash
    {
        size_t operator()(const ResourceLink & link) const
        {
            return std::hash<ISchedulerQueue*>()(link.queue);
        }
    };

    std::mutex link_data_mutex;
    std::unordered_map<ResourceLink, TItem, ResourceQueueHash> link_data;

    explicit ResourceTestManager(size_t thread_count = 1)
        : manager(new TManager)
        , busy_period(thread_count)
    {}

    ~ResourceTestManager()
    {
        for (auto & thread : threads)
            thread.join();
    }

    void update(const String & xml)
    {
        std::istringstream stream(xml); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::AutoPtr config{new Poco::Util::XMLConfiguration(stream)};
        manager->updateConfiguration(*config);
    }

    auto & getLinkData(ResourceLink link)
    {
        std::unique_lock lock{link_data_mutex};
        return link_data[link];
    }

    // Use exactly two threads for each queue to avoid queue being deactivated (happens with 1 thread) and reordering (happens with >2 threads):
    // while the first request is executing, the second request is in queue - holding it active.
    // use onEnqueue() and onExecute() functions for this purpose.
    void onEnqueue(ResourceLink link)
    {
        getLinkData(link).enqueued.fetch_add(1, std::memory_order_relaxed);
    }
    void onExecute(ResourceLink link)
    {
        auto & data = getLinkData(link);
        Int64 left = data.left.fetch_sub(1, std::memory_order_relaxed) - 1;
        Int64 enqueued = data.enqueued.fetch_sub(1, std::memory_order_relaxed) - 1;
        while (left > 0 && enqueued <= 0) // Ensure at least one thread has already enqueued itself (or there is no more requests)
        {
            std::this_thread::yield();
            left = data.left.load();
            enqueued = data.enqueued.load();
        }
    }

    // This is required for proper busy period start, i.e. everyone to be seen by scheduler as appeared at the same time:
    //  - resource is blocked with queries by leader thread;
    //  - leader thread notifies followers to enqueue their requests;
    //  - leader thread unblocks resource;
    //  - busy period begins.
    // NOTE: actually leader's request(s) make their own small busy period.
    void blockResource(ResourceLink link)
    {
        ResourceGuard g(ResourceGuard::Metrics::getIOWrite(), link, 1, ResourceGuard::Lock::Defer);
        g.lock();
        g.consume(1);
        // NOTE: at this point we assume resource to be blocked by single request (<max_requests>1</max_requests>)
        busy_period.arrive_and_wait(); // (1) notify all followers that resource is blocked
        busy_period.arrive_and_wait(); // (2) wait all followers to enqueue their requests
    }
    void startBusyPeriod(ResourceLink link, ResourceCost cost, Int64 total_requests)
    {
        getLinkData(link).left += total_requests + 1;
        busy_period.arrive_and_wait(); // (1) wait leader to block resource
        ResourceGuard g(ResourceGuard::Metrics::getIOWrite(), link, cost, ResourceGuard::Lock::Defer);
        onEnqueue(link);
        busy_period.arrive_and_wait(); // (2) notify leader to unblock
        g.lock();
        g.consume(cost);
        onExecute(link);
    }
};

}
