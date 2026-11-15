#include <Interpreters/Context.h>
#include <Access/AccessControl.h>
#include <Access/SettingsProfile.h>
#include <Common/tests/gtest_global_context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Core/Field.h>
#include <base/scope_guard.h>
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
    /// `Context::createCopy` shares the `query_privileges_info` pointer with `getContext().context`,
    /// so give the parent its own accumulator first — otherwise `addQueryPrivilegesInfo` below would
    /// write the fake privileges through to the process-global context and make later gtests that copy
    /// it (without calling `makeQueryContext`) order-dependent.
    auto parent_context = Context::createCopy(getContext().context);
    parent_context->makeQueryContext();
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

/// Test for a data race on the Settings block between Context::getReadSettings() /
/// Context::getWriteSettings() and a concurrent settings write.
///
/// The writers this test exercises (`Context::setSetting` -> `setSettingWithLock`) hold
/// Context::mutex exclusively, while the two getters read dozens of settings between them through
/// the unsynchronized `getSettingsRef()` accessor. A background thread with no query context resolves free
/// `DB::getReadSettings()` to the global context, so it reads the same block a settings write
/// mutates. `local_filesystem_read_method` is a `SettingFieldString`, so the reader can also
/// observe a torn `std::string`.
///
/// ThreadSanitizer reports races per memory address, so the writer mutates a setting behind every
/// read that is expected to be synchronized: the ones the two getters read directly, and the four
/// bandwidths. The readers also call the four throttler getters directly, because that is the only
/// path on which a getter reads a bandwidth under its own lock: from `getReadSettings` and
/// `getWriteSettings` the value arrives as an argument. Keeping all four bandwidths non-zero also
/// makes every throttler getter take its own exclusive lock on each call, which must not overlap the
/// settings read, or this test deadlocks.
///
/// This test only carries signal on a ThreadSanitizer build, where it reports a race between
/// `Context::getReadSettings` and `SettingsImpl::set` without the fix. On every other build it
/// passes either way.
TEST(Context, GetReadSettingsRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    context->setSetting("max_local_read_bandwidth", Field(UInt64(1000000)));
    context->setSetting("max_remote_read_network_bandwidth", Field(UInt64(1000000)));
    context->setSetting("max_local_write_bandwidth", Field(UInt64(1000000)));
    context->setSetting("max_remote_write_network_bandwidth", Field(UInt64(1000000)));

    constexpr size_t num_reader_threads = 4;
    constexpr size_t num_iterations = 1000;
    std::atomic<bool> stop{false};

    std::vector<std::thread> readers;
    for (size_t i = 0; i < num_reader_threads; ++i)
    {
        readers.emplace_back([&context, &stop]
        {
            while (!stop.load(std::memory_order_relaxed))
            {
                try
                {
                    (void)context->getReadSettings();
                    (void)context->getWriteSettings();
                    (void)context->getRemoteReadThrottler();
                    (void)context->getLocalReadThrottler();
                    (void)context->getRemoteWriteThrottler();
                    (void)context->getLocalWriteThrottler();
                }
                catch (...) // Ok: a torn read of a string setting throws UNKNOWN_READ_METHOD  // NOLINT(bugprone-empty-catch)
                {
                }
            }
        });
    }

    /// `local_filesystem_read_method` alternates between two valid `LocalFSReadMethod` names, so the
    /// value the reader parses is well-formed unless it is torn.
    /// `s3_allow_parallel_part_upload` is read by `getWriteSettings` and not by `getReadSettings`, so
    /// each getter races against a setting only it reads.
    /// The bandwidths alternate between two non-zero values: zero would skip the throttler getters'
    /// exclusive branch on half the iterations.
    std::thread writer([&context, &stop]
    {
        for (size_t i = 0; i < num_iterations; ++i)
        {
            context->setSetting("local_filesystem_read_method", Field(String(i % 2 ? "pread" : "read")));
            context->setSetting("s3_allow_parallel_part_upload", Field(UInt64(i % 2)));

            const UInt64 bandwidth = i % 2 ? 1000000 : 2000000;
            context->setSetting("max_local_read_bandwidth", Field(bandwidth));
            context->setSetting("max_remote_read_network_bandwidth", Field(bandwidth));
            context->setSetting("max_local_write_bandwidth", Field(bandwidth));
            context->setSetting("max_remote_write_network_bandwidth", Field(bandwidth));
        }
        stop.store(true, std::memory_order_relaxed);
    });

    writer.join();
    for (auto & t : readers)
        t.join();
}

/// Test for a data race on the Settings block between Context::setDefaultProfiles() and a
/// concurrent settings write.
///
/// `setDefaultProfiles` re-applies `applySettingsQuirks`, `adjustSettingsForMakeDistributedPlan`
/// and `doSettingsSanityCheckClamp` to `*settings` after `setCurrentProfile` has released
/// Context::mutex. `setCurrentProfile` already ran all three under the lock, so in a resolved
/// profile the trailing pass changes nothing and only *reads* the settings it inspects -- which is
/// why the concurrent thread here is a writer holding the mutex, not another reader.
///
/// `setCurrentProfile` orders this thread after every earlier writer, so the race window is the
/// interval between that release and `makeBackgroundContext` taking the mutex again. The writer
/// threads target `max_read_buffer_size` and three siblings, all read by
/// `doSettingsSanityCheckClamp`, and stay below its clamp ceilings so the values survive.
///
/// `makeBackgroundContext` asserts on the process-wide static `background_context_instance`, so
/// `setDefaultProfiles` can succeed at most once per binary: this is the only test that calls it,
/// and it must not be repeated within one process.
///
/// This test only carries signal on a ThreadSanitizer build. On every other build it passes either
/// way.
TEST(Context, SetDefaultProfilesRace)
{
    auto context = Context::createCopy(getContext().context);

    /// The fixture's AccessControl has no storage, so `default` would not resolve and
    /// `setCurrentProfile` would throw `Settings profile 'default' not found`. An empty element list
    /// is enough: the profile only has to resolve.
    ///
    /// `createCopy` shares the fixture's ContextSharedPart, so this AccessControl is the one every
    /// other test in this binary sees. All three steps below are needed to restore it: dropping the
    /// storage does not evict the profile from `SettingsProfilesCache`, and removing the profile does
    /// not reset the cached default profile id.
    auto & access_control = context->getAccessControl();
    access_control.addMemoryStorage("test_memory", /*allow_backup_=*/false);
    auto profile = std::make_shared<SettingsProfile>();
    profile->setName("default");
    const UUID profile_id = access_control.insert(profile);
    SCOPE_EXIT({
        access_control.setDefaultProfileName({});
        access_control.remove(profile_id, /*throw_if_not_exists=*/false);
        if (auto storage = access_control.findStorageByName("test_memory"))
            access_control.removeStorage(storage);
    });

    /// No <background_profile>: that takes the branch of makeBackgroundContext which reuses the
    /// system profile and needs no second profile to exist.
    std::istringstream config_stream{"<clickhouse><default_profile>default</default_profile></clickhouse>"};
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(config_stream);

    constexpr size_t num_writer_threads = 4;
    std::atomic<bool> stop{false};
    std::atomic<size_t> writes{0};

    std::vector<std::thread> writers;
    for (size_t i = 0; i < num_writer_threads; ++i)
    {
        writers.emplace_back([&context, &stop, &writes]
        {
            size_t iteration = 0;
            while (!stop.load(std::memory_order_relaxed))
            {
                const UInt64 size = ++iteration % 2 ? 1048576 : 2097152;
                context->setSetting("max_read_buffer_size", Field(size));
                context->setSetting("max_read_buffer_size_local_fs", Field(size));
                context->setSetting("max_read_buffer_size_remote_fs", Field(size));
                context->setSetting("prefetch_buffer_size", Field(size));
                writes.fetch_add(4, std::memory_order_relaxed);
            }
        });
    }

    /// Let the writers reach steady state, so a write to each of the four addresses is in
    /// ThreadSanitizer's shadow history when the unsynchronized pass reads them.
    while (writes.load(std::memory_order_relaxed) < 10000)
        std::this_thread::yield();

    context->setDefaultProfiles(*config);

    stop.store(true, std::memory_order_relaxed);
    for (auto & t : writers)
        t.join();
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
