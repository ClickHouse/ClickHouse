#pragma once

#include <gtest/gtest.h>

#include <Common/Scheduler/SchedulingSettings.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/SchedulerRoot.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Scheduler/Nodes/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/FifoQueue.h>
#include <Common/Scheduler/Nodes/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/UnifiedSchedulerNode.h>
#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>

#include <Poco/Util/XMLConfiguration.h>

#include <atomic>
#include <barrier>
#include <exception>
#include <functional>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <set>
#include <sstream>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
}

struct ResourceTestBase
{
    ResourceTestBase()
    {
        [[maybe_unused]] static bool typesRegistered = [] { registerSchedulerNodes(); return true; }();
    }

    template <class TClass>
    static TClass * add(EventQueue * event_queue, SchedulerNodePtr & root_node, const String & path, const String & xml = {})
    {
        std::stringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        stream << "<resource><node path=\"" << path << "\">" << xml << "</node></resource>";
        Poco::AutoPtr config{new Poco::Util::XMLConfiguration(stream)};
        String config_prefix = "node";

        return add<TClass>(event_queue, root_node, path, std::ref(*config), config_prefix);
    }

    template <class TClass, class... Args>
    static TClass * add(EventQueue * event_queue, SchedulerNodePtr & root_node, const String & path, Args... args)
    {
        if (path == "/")
        {
            EXPECT_TRUE(root_node.get() == nullptr);
            root_node.reset(new TClass(event_queue, std::forward<Args>(args)...));
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
        SchedulerNodePtr node = std::make_shared<TClass>(event_queue, std::forward<Args>(args)...);
        node->basename = child_name;
        parent->attachChild(node);
        return static_cast<TClass *>(node.get());
    }
};

class ResourceTestClass : public ResourceTestBase
{
    struct Request : public ResourceRequest
    {
        ResourceTestClass * test;
        String name;

        Request(ResourceTestClass * test_, ResourceCost cost_, const String & name_)
            : ResourceRequest(cost_)
            , test(test_)
            , name(name_)
        {}

        void execute() override
        {
        }

        void failed(const std::exception_ptr &) override
        {
            test->failed_cost += cost;
            delete this;
        }
    };

public:
    ~ResourceTestClass()
    {
        if (root_node)
            dequeue(); // Just to avoid any leaks of `Request` object
    }

    template <class TClass>
    void add(const String & path, const String & xml = {})
    {
        ResourceTestBase::add<TClass>(&event_queue, root_node, path, xml);
    }

    template <class TClass, class... Args>
    void addCustom(const String & path, Args... args)
    {
        ResourceTestBase::add<TClass>(&event_queue, root_node, path, std::forward<Args>(args)...);
    }

    UnifiedSchedulerNodePtr createUnifiedNode(const String & basename, const SchedulingSettings & settings = {})
    {
        return createUnifiedNode(basename, {}, settings);
    }

    UnifiedSchedulerNodePtr createUnifiedNode(const String & basename, const UnifiedSchedulerNodePtr & parent, const SchedulingSettings & settings = {})
    {
        auto node = std::make_shared<UnifiedSchedulerNode>(&event_queue, settings);
        node->basename = basename;
        if (parent)
        {
            parent->attachUnifiedChild(node);
        }
        else
        {
            EXPECT_TRUE(root_node.get() == nullptr);
            root_node = node;
        }
        return node;
    }

    // Updates the parent and/or scheduling settings for a specidfied `node`.
    // Unit test implementation must make sure that all needed queues and constraints are not going to be destroyed.
    // Normally it is the responsibility of IOResourceManager, but we do not use it here, so manual version control is required.
    // (see IOResourceManager::Resource::updateCurrentVersion() fo details)
    void updateUnifiedNode(const UnifiedSchedulerNodePtr & node, const UnifiedSchedulerNodePtr & old_parent, const UnifiedSchedulerNodePtr & new_parent, const SchedulingSettings & new_settings)
    {
        EXPECT_TRUE((old_parent && new_parent) || (!old_parent && !new_parent)); // changing root node is not supported
        bool detached = false;
        if (UnifiedSchedulerNode::updateRequiresDetach(
            old_parent ? old_parent->basename : "",
            new_parent ? new_parent->basename : "",
            node->getSettings(),
            new_settings))
        {
            if (old_parent)
                old_parent->detachUnifiedChild(node);
            detached = true;
        }

        node->updateSchedulingSettings(new_settings);

        if (detached && new_parent)
            new_parent->attachUnifiedChild(node);
    }


    void enqueue(const UnifiedSchedulerNodePtr & node, const std::vector<ResourceCost> & costs)
    {
        enqueueImpl(node->getQueue().get(), costs, node->basename);
    }

    void enqueue(const String & path, const std::vector<ResourceCost> & costs)
    {
        ASSERT_TRUE(root_node.get() != nullptr); // root should be initialized first
        ISchedulerNode * node = root_node.get();
        size_t pos = 1;
        while (node && pos < path.length())
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
        if (node)
            enqueueImpl(dynamic_cast<ISchedulerQueue *>(node), costs);
    }

    void enqueueImpl(ISchedulerQueue * queue, const std::vector<ResourceCost> & costs, const String & name = {})
    {
        ASSERT_TRUE(queue != nullptr); // not a queue
        if (!queue)
            return; // to make clang-analyzer-core.NonNullParamChecker happy
        for (ResourceCost cost : costs)
            queue->enqueueRequest(new Request(this, cost, name.empty() ? queue->basename : name));
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

    void failed(ResourceCost value)
    {
        EXPECT_EQ(failed_cost, value);
        failed_cost -= value;
    }

    void processEvents()
    {
        while (event_queue.tryProcess()) {}
    }

private:
    EventQueue event_queue;
    SchedulerNodePtr root_node;
    std::unordered_map<String, ResourceCost> consumed_cost;
    ResourceCost failed_cost = 0;
};

enum EnqueueOnlyEnum { EnqueueOnly };

template <class TManager>
struct ResourceTestManager : public ResourceTestBase
{
    ResourceManagerPtr manager;

    std::vector<ThreadFromGlobalPool> threads;
    std::barrier<> busy_period;

    struct Guard : public ResourceGuard
    {
        ResourceTestManager & t;
        ResourceCost cost;

        /// Works like regular ResourceGuard, ready for consumption after constructor
        Guard(ResourceTestManager & t_, ResourceLink link_, ResourceCost cost_)
            : ResourceGuard(ResourceGuard::Metrics::getIOWrite(), link_, cost_, Lock::Defer)
            , t(t_)
            , cost(cost_)
        {
            t.onEnqueue(link);
            waitExecute();
        }

        /// Just enqueue resource request, do not block (needed for tests to sync). Call `waitExecuted()` afterwards
        Guard(ResourceTestManager & t_, ResourceLink link_, ResourceCost cost_, EnqueueOnlyEnum)
            : ResourceGuard(ResourceGuard::Metrics::getIOWrite(), link_, cost_, Lock::Defer)
            , t(t_)
            , cost(cost_)
        {
            t.onEnqueue(link);
        }

        /// Waits for ResourceRequest::execute() to be called for enqueued request
        void waitExecute()
        {
            lock();
            t.onExecute(link);
            consume(cost);
        }

        /// Waits for ResourceRequest::failure() to be called for enqueued request
        void waitFailed(const String & pattern)
        {
            try
            {
                lock();
                FAIL();
            }
            catch (Exception & e)
            {
                ASSERT_EQ(e.code(), ErrorCodes::RESOURCE_ACCESS_DENIED);
                ASSERT_TRUE(e.message().contains(pattern));
            }
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

    enum DoNotInitManagerEnum { DoNotInitManager };

    explicit ResourceTestManager(size_t thread_count, DoNotInitManagerEnum)
        : busy_period(thread_count)
    {}

    ~ResourceTestManager()
    {
        wait();
    }

    void wait()
    {
        for (auto & thread : threads)
        {
            if (thread.joinable())
                thread.join();
        }
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
