#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Core/Settings.h>

#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Common/Scheduler/Nodes/IOResourceManager.h>

#include <Interpreters/Context.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTDropWorkloadQuery.h>
#include <Parsers/ASTDropResourceQuery.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>
#include <Parsers/ParserDropWorkloadQuery.h>
#include <Parsers/ParserDropResourceQuery.h>

using namespace DB;

class WorkloadEntityTestStorage : public WorkloadEntityStorageBase
{
public:
    WorkloadEntityTestStorage()
        : WorkloadEntityStorageBase(Context::getGlobalContextInstance())
    {}

    void loadEntities() override {}

    void executeQuery(const String & query)
    {
        ParserCreateWorkloadQuery create_workload_p;
        ParserDropWorkloadQuery drop_workload_p;
        ParserCreateResourceQuery create_resource_p;
        ParserDropResourceQuery drop_resource_p;

        auto parse = [&] (IParser & parser)
        {
            String error;
            const char * end = query.data();
            return tryParseQuery(
                parser,
                end,
                query.data() + query.size(),
                error,
                false,
                "",
                false,
                0,
                DBMS_DEFAULT_MAX_PARSER_DEPTH,
                DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
                true);
        };

        if (ASTPtr create_workload = parse(create_workload_p))
        {
            auto & parsed = create_workload->as<ASTCreateWorkloadQuery &>();
            auto workload_name = parsed.getWorkloadName();
            bool throw_if_exists = !parsed.if_not_exists && !parsed.or_replace;
            bool replace_if_exists = parsed.or_replace;

            storeEntity(
                nullptr,
                WorkloadEntityType::Workload,
                workload_name,
                create_workload,
                throw_if_exists,
                replace_if_exists,
                {});
        }
        else if (ASTPtr create_resource = parse(create_resource_p))
        {
            auto & parsed = create_resource->as<ASTCreateResourceQuery &>();
            auto resource_name = parsed.getResourceName();
            bool throw_if_exists = !parsed.if_not_exists && !parsed.or_replace;
            bool replace_if_exists = parsed.or_replace;

            storeEntity(
                nullptr,
                WorkloadEntityType::Resource,
                resource_name,
                create_resource,
                throw_if_exists,
                replace_if_exists,
                {});
        }
        else if (ASTPtr drop_workload = parse(drop_workload_p))
        {
            auto & parsed = drop_workload->as<ASTDropWorkloadQuery &>();
            bool throw_if_not_exists = !parsed.if_exists;
            removeEntity(
                nullptr,
                WorkloadEntityType::Workload,
                parsed.workload_name,
                throw_if_not_exists);
        }
        else if (ASTPtr drop_resource = parse(drop_resource_p))
        {
            auto & parsed = drop_resource->as<ASTDropResourceQuery &>();
            bool throw_if_not_exists = !parsed.if_exists;
            removeEntity(
                nullptr,
                WorkloadEntityType::Resource,
                parsed.resource_name,
                throw_if_not_exists);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid query in WorkloadEntityTestStorage: {}", query);
    }

private:
    WorkloadEntityStorageBase::OperationResult storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override
    {
        UNUSED(current_context, entity_type, entity_name, create_entity_query, throw_if_exists, replace_if_exists, settings);
        return OperationResult::Ok;
    }

    WorkloadEntityStorageBase::OperationResult removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override
    {
        UNUSED(current_context, entity_type, entity_name, throw_if_not_exists);
        return OperationResult::Ok;
    }
};

struct ResourceTest : ResourceTestManager<IOResourceManager>
{
    WorkloadEntityTestStorage storage;

    explicit ResourceTest(size_t thread_count = 1)
        : ResourceTestManager(thread_count, DoNotInitManager)
    {
        manager = std::make_shared<IOResourceManager>(storage);
    }

    void query(const String & query_str)
    {
        storage.executeQuery(query_str);
    }

    template <class Func>
    void async(const String & workload, Func func)
    {
        threads.emplace_back([=, this, func2 = std::move(func)]
        {
            ClassifierPtr classifier = manager->acquire(workload);
            func2(classifier);
        });
    }

    template <class Func>
    void async(const String & workload, const String & resource, Func func)
    {
        threads.emplace_back([=, this, func2 = std::move(func)]
        {
            ClassifierPtr classifier = manager->acquire(workload);
            ResourceLink link = classifier->get(resource);
            func2(link);
        });
    }
};

using TestGuard = ResourceTest::Guard;

TEST(SchedulerIOResourceManager, Smoke)
{
    ResourceTest t;

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_requests = 10");
    t.query("CREATE WORKLOAD A in all");
    t.query("CREATE WORKLOAD B in all SETTINGS weight = 3");

    ClassifierPtr c_a = t.manager->acquire("A");
    ClassifierPtr c_b = t.manager->acquire("B");

    for (int i = 0; i < 10; i++)
    {
        ResourceGuard g_a(ResourceGuard::Metrics::getIOWrite(), c_a->get("res1"), 1, ResourceGuard::Lock::Defer);
        g_a.lock();
        g_a.consume(1);
        g_a.unlock();

        ResourceGuard g_b(ResourceGuard::Metrics::getIOWrite(), c_b->get("res1"));
        g_b.unlock();

        ResourceGuard g_c(ResourceGuard::Metrics::getIORead(), c_b->get("res1"));
        g_b.consume(2);
    }
}

TEST(SchedulerIOResourceManager, Fairness)
{
    // Total cost for A and B cannot differ for more than 1 (every request has cost equal to 1).
    // Requests from A use `value = 1` and from B `value = -1` is used.
    std::atomic<Int64> unfairness = 0;
    auto fairness_diff = [&] (Int64 value)
    {
        Int64 cur_unfairness = unfairness.fetch_add(value, std::memory_order_relaxed) + value;
        EXPECT_NEAR(cur_unfairness, 0, 1);
    };

    constexpr size_t threads_per_queue = 2;
    int requests_per_thread = 100;
    ResourceTest t(2 * threads_per_queue + 1);

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_requests = 1");
    t.query("CREATE WORKLOAD A IN all");
    t.query("CREATE WORKLOAD B IN all");
    t.query("CREATE WORKLOAD leader IN all");

    for (int thread = 0; thread < threads_per_queue; thread++)
    {
        t.threads.emplace_back([&]
        {
            ClassifierPtr c = t.manager->acquire("A");
            ResourceLink link = c->get("res1");
            t.startBusyPeriod(link, 1, requests_per_thread);
            for (int request = 0; request < requests_per_thread; request++)
            {
                TestGuard g(t, link, 1);
                fairness_diff(1);
            }
        });
    }

    for (int thread = 0; thread < threads_per_queue; thread++)
    {
        t.threads.emplace_back([&]
        {
            ClassifierPtr c = t.manager->acquire("B");
            ResourceLink link = c->get("res1");
            t.startBusyPeriod(link, 1, requests_per_thread);
            for (int request = 0; request < requests_per_thread; request++)
            {
                TestGuard g(t, link, 1);
                fairness_diff(-1);
            }
        });
    }

    ClassifierPtr c = t.manager->acquire("leader");
    ResourceLink link = c->get("res1");
    t.blockResource(link);

    t.wait(); // Wait for threads to finish before destructing locals
}

TEST(SchedulerIOResourceManager, DropNotEmptyQueue)
{
    ResourceTest t;

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_requests = 1");
    t.query("CREATE WORKLOAD intermediate IN all");

    std::barrier sync_before_enqueue(2);
    std::barrier sync_before_drop(3);
    std::barrier sync_after_drop(2);
    t.async("intermediate", "res1", [&] (ResourceLink link)
    {
        TestGuard g(t, link, 1);
        sync_before_enqueue.arrive_and_wait();
        sync_before_drop.arrive_and_wait(); // 1st resource request is consuming
        sync_after_drop.arrive_and_wait(); // 1st resource request is still consuming
    });

    sync_before_enqueue.arrive_and_wait(); // to maintain correct order of resource requests

    t.async("intermediate", "res1", [&] (ResourceLink link)
    {
        TestGuard g(t, link, 1, EnqueueOnly);
        sync_before_drop.arrive_and_wait(); // 2nd resource request is enqueued
        g.waitFailed("is about to be destructed");
    });

    sync_before_drop.arrive_and_wait(); // main thread triggers FifoQueue destruction by adding a unified child
    t.query("CREATE WORKLOAD leaf IN intermediate");
    sync_after_drop.arrive_and_wait();

    t.wait(); // Wait for threads to finish before destructing locals
}

TEST(SchedulerIOResourceManager, DropNotEmptyQueueLong)
{
    ResourceTest t;

    t.query("CREATE RESOURCE res1 (WRITE DISK disk, READ DISK disk)");
    t.query("CREATE WORKLOAD all SETTINGS max_requests = 1");
    t.query("CREATE WORKLOAD intermediate IN all");

    static constexpr int queue_size = 100;
    std::barrier sync_before_enqueue(2);
    std::barrier sync_before_drop(2 + queue_size);
    std::barrier sync_after_drop(2);
    t.async("intermediate", "res1", [&] (ResourceLink link)
    {
        TestGuard g(t, link, 1);
        sync_before_enqueue.arrive_and_wait();
        sync_before_drop.arrive_and_wait(); // 1st resource request is consuming
        sync_after_drop.arrive_and_wait(); // 1st resource request is still consuming
    });

    sync_before_enqueue.arrive_and_wait(); // to maintain correct order of resource requests

    for (int i = 0; i < queue_size; i++)
    {
        t.async("intermediate", "res1", [&] (ResourceLink link)
        {
            TestGuard g(t, link, 1, EnqueueOnly);
            sync_before_drop.arrive_and_wait(); // many resource requests are enqueued
            g.waitFailed("is about to be destructed");
        });
    }

    sync_before_drop.arrive_and_wait(); // main thread triggers FifoQueue destruction by adding a unified child
    t.query("CREATE WORKLOAD leaf IN intermediate");
    sync_after_drop.arrive_and_wait();

    t.wait(); // Wait for threads to finish before destructing locals
}
