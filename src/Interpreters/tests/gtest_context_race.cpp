#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Core/Field.h>
#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include <sstream>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

template <typename Ptr>
void run(Ptr context)
{
    for (size_t i = 0; i < 100; ++i)
    {
        std::thread t1([context]
        {
            if constexpr (std::is_same_v<ContextWeakPtr, Ptr>)
                context.lock()->getAsyncReadCounters();
            else
                context->getAsyncReadCounters();
        });

        std::thread t2([context]
        {
            Context::createCopy(context);
        });

        t1.join();
        t2.join();
    }
}

TEST(Context, MutableRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextMutablePtr>(context);
}

TEST(Context, ConstRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextPtr>(context);
}

TEST(Context, WeakRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextWeakPtr>(context);
}

/// Test for data race in Context::getAccess() where need_recalculate_access
/// was written under a shared lock while being read by another thread.
/// Multiple threads call getAccess() on the same context while another thread
/// toggles need_recalculate_access via setSetting with an access-dependent setting.
TEST(Context, GetAccessRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// Populate the cached access object.
    context->getAccess();

    constexpr size_t num_reader_threads = 4;
    constexpr size_t num_iterations = 1000;
    std::atomic<bool> stop{false};

    /// Reader threads: call getAccess() concurrently on the same context.
    std::vector<std::thread> readers;
    for (size_t i = 0; i < num_reader_threads; ++i)
    {
        readers.emplace_back([&context, &stop]
        {
            while (!stop.load(std::memory_order_relaxed))
                context->getAccess();
        });
    }

    /// Writer thread: toggle need_recalculate_access by setting allow_ddl
    /// (one of the three settings in ContextAccessParams::dependsOnSettingName).
    std::thread writer([&context, &stop]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            context->setSetting("allow_ddl", Field(UInt64(1)));
        stop.store(true, std::memory_order_relaxed);
    });

    writer.join();
    for (auto & t : readers)
        t.join();
}

/// Test for data race in `ContextData` copy constructor on `table_function_results`.
///
/// The writer thread calls `Context::executeTableFunction`, which mutates
/// `table_function_results` under `table_function_results_mutex`.
/// The copier thread calls `Context::createCopy`, which invokes the
/// `ContextData(const ContextData &)` copy constructor.
///
/// Without the fix the copy constructor read `o.table_function_results`
/// in its initializer list without acquiring `o.table_function_results_mutex`,
/// and TSan reported a data race against the writer's `emplace`. With the fix
/// the copy of `table_function_results` happens under that mutex.
///
/// See issue ClickHouse/ClickHouse#104807 (STID 1003-358c).
TEST(Context, TableFunctionResultsCopyRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// Warm up the table-function machinery with a literal (`numbers(0)`)
    /// distinct from the ones the writer loop will use, so the writer still
    /// hits the cache-miss insertion path on every iteration.
    try
    {
        auto warmup_ast = makeASTFunction("numbers", make_intrusive<ASTLiteral>(Field(UInt64(0))));
        (void)context->executeTableFunction(warmup_ast);
    }
    catch (...) // Ok: ignore execution failures, we only care about exercising the cache path  // NOLINT(bugprone-empty-catch)
    {
    }

    constexpr size_t num_iterations = 200;
    std::atomic<bool> stop{false};

    /// Writer thread: vary the integer literal each iteration so the
    /// `executeTableFunction` call always hashes to a fresh key. This forces
    /// the cache-miss path that inserts into `table_function_results` (under
    /// `table_function_results_mutex`) on every iteration, which is the write
    /// the copier thread must race against. Calling with the same AST would
    /// hit the cache after the first call and not mutate the map -- making
    /// the race read-vs-read and invisible to TSan even without the fix.
    /// See the bot's inline review on PR #104879.
    std::thread writer([&]
    {
        UInt64 i = 1;
        while (!stop.load(std::memory_order_relaxed))
        {
            auto ast = makeASTFunction("numbers", make_intrusive<ASTLiteral>(Field(i++)));
            try
            {
                (void)context->executeTableFunction(ast);
            }
            catch (...) // Ok: ignore execution failures, we only care about exercising the cache path  // NOLINT(bugprone-empty-catch)
            {
            }
        }
    });

    /// Copier thread: keep copying the context, which invokes the
    /// `ContextData` copy constructor that reads `o.table_function_results`.
    std::thread copier([&]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            (void)Context::createCopy(context);
        stop.store(true, std::memory_order_relaxed);
    });

    copier.join();
    writer.join();
}

/// Regression test for a cross-query leak in `system.query_log.used_privileges`.
///
/// A query context is created via `createCopy(session_or_global_context)` followed by
/// `makeQueryContext()`. Session contexts are themselves `createCopy(global_context)` and never
/// call `makeQueryContext`, so the session and global contexts share a single `QueryPrivilegesInfo`
/// object. `makeQueryContext` used to seed the new query's privileges by *copying the contents* of
/// that shared parent object (`std::make_shared<QueryPrivilegesInfo>(*query_privileges_info)`).
///
/// As a result, any privilege string that ever landed in the shared parent object (and, under
/// concurrency, partial state observed mid-write) leaked into the `used_privileges` of unrelated
/// later queries from other sessions and databases. The fix seeds every query with an empty
/// `QueryPrivilegesInfo`. This test models the scenario deterministically: it pollutes a parent
/// context's privileges, derives a query context from it, and asserts the query starts clean.
///
/// See issue ClickHouse/ClickHouse#105983.
TEST(Context, MakeQueryContextDoesNotInheritPrivileges)
{
    /// Stand in for the shared session/global context whose `QueryPrivilegesInfo` gets polluted.
    auto parent_context = Context::createCopy(getContext().context);
    parent_context->addQueryPrivilegesInfo("SELECT(naughty_column) ON some_db.some_table", true);
    parent_context->addQueryPrivilegesInfo("INSERT ON some_db.some_table", false);

    {
        const auto & parent_info = parent_context->getQueryPrivilegesInfo();
        std::lock_guard lock(parent_info.mutex);
        ASSERT_FALSE(parent_info.used_privileges.empty());
        ASSERT_FALSE(parent_info.missing_privileges.empty());
    }

    /// A new query context, as created by `Session::makeQueryContextImpl`.
    auto query_context = Context::createCopy(parent_context);
    query_context->makeQueryContext();

    /// The fresh query must not have inherited any of the parent's privilege strings.
    {
        const auto & query_info = query_context->getQueryPrivilegesInfo();
        std::lock_guard lock(query_info.mutex);
        EXPECT_TRUE(query_info.used_privileges.empty());
        EXPECT_TRUE(query_info.missing_privileges.empty());
    }

    /// Privileges checked during the new query must be tracked independently of the parent,
    /// and must not bleed back into the parent's (shared) object.
    query_context->addQueryPrivilegesInfo("SELECT ON my_db.my_table", true);
    {
        const auto & query_info = query_context->getQueryPrivilegesInfo();
        std::lock_guard lock(query_info.mutex);
        EXPECT_EQ(query_info.used_privileges.count("SELECT ON my_db.my_table"), 1u);
    }
    {
        const auto & parent_info = parent_context->getQueryPrivilegesInfo();
        std::lock_guard lock(parent_info.mutex);
        EXPECT_EQ(parent_info.used_privileges.count("SELECT ON my_db.my_table"), 0u);
    }
}

/// Regression test for a startup race between DNSCacheUpdater and ConfigReloader.
///
/// DNSCacheUpdater::run() can call Context::reloadClusterConfig() before the first
/// ConfigReloader pass stores a ConfigurationPtr into shared->clusters_config.
/// reloadClusterConfig() falls back to getConfigRef() and sets shared->clusters (non-null)
/// but leaves shared->clusters_config null. When setClustersConfig() subsequently runs
/// it used to dereference shared->clusters_config unconditionally whenever shared->clusters
/// was non-null, throwing Poco::NullPointerException.
///
/// The fix adds a shared->clusters_config null-guard before the isSameConfiguration() call.
/// This test reproduces the exact call order deterministically (no real threading needed)
/// and fails without the fix.
TEST(Context, SetClustersConfigAfterReloadClusterConfig)
{
    auto context = Context::createCopy(getContext().context);

    /// Simulate DNSCacheUpdater firing before ConfigReloader: this populates
    /// shared->clusters via the getConfigRef() fallback, leaving clusters_config null.
    ASSERT_NO_THROW(context->reloadClusterConfig());

    /// Now simulate the first ConfigReloader pass calling setClustersConfig().
    /// Without the fix this throws Poco::NullPointerException because shared->clusters
    /// is non-null but shared->clusters_config is still null.
    std::istringstream config_stream{"<clickhouse><remote_servers/></clickhouse>"};
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(config_stream);
    ASSERT_NO_THROW(context->setClustersConfig(config, /*enable_discovery=*/false));

    /// Verify the recovered state is sane: getClusters() must not throw and must
    /// reflect the config we just applied (empty <remote_servers> → empty map).
    std::map<String, ClusterPtr> clusters;
    ASSERT_NO_THROW(clusters = context->getClusters());
    EXPECT_TRUE(clusters.empty());
}
