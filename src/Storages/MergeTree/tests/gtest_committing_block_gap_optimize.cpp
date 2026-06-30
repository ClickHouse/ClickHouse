#include <gtest/gtest.h>

#include <filesystem>
#include <sstream>
#include <string>

#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Common/Config/ConfigProcessor.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/registerDatabases.h>
#include <Disks/registerDisks.h>
#include <Storages/registerStorages.h>
#include <IO/ReadBufferFromString.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/registerInterpreters.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/StorageMergeTree.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

namespace
{

/// One-time bootstrap of a minimal in-process server on top of the single shared test context
/// (creating a second ContextShared is forbidden, so we reuse the global one): registers
/// everything an OPTIMIZE on a real MergeTree needs and attaches a default DatabaseMemory.
ContextMutablePtr setUpContext()
{
    /// getMutableContext() owns the one and only ContextShared for the whole test binary; other
    /// gtests may have built it already. Reuse it and finish the server-style setup exactly once.
    ContextMutablePtr context = getMutableContext().context;

    static bool initialized = [&]
    {
        auto make_config = [](const char * xml)
        {
            std::stringstream ss{std::string{xml}}; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::XML::InputSource input_source{ss};
            return ConfigurationPtr{new Poco::Util::XMLConfiguration{&input_source}};
        };

        context->setConfig(make_config("<clickhouse></clickhouse>"));

        /// MergeTree parts and OPTIMIZE merges land on disk under this path. Wipe any leftovers
        /// from an earlier run so CREATE TABLE does not trip over a stale data directory.
        const auto path = std::filesystem::temp_directory_path() / "gtest_committing_block_gap_optimize" / "";
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
        context->setPath(path.string());

        MainThreadStatus::getInstance();
        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        registerInterpreters();
        /// Functions, aggregate functions and formats register into process-wide factories that
        /// other gtests in this same unit_tests_dbms binary also populate. The plain register*()
        /// helpers throw on a second registration, so go through the shared try-once wrappers
        /// (gtest_global_register.h) which register at most once per process.
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();
        tryRegisterFormats();
        registerDatabases();
        registerStorages();
        registerDisks(/*global_skip_access_check=*/true);

        /// A default profile and user are required before running queries: ProcessList and settings
        /// application resolve them. Use the same minimal users config clickhouse-local uses.
        static const char * minimal_users_xml =
            "<clickhouse>"
            "    <profiles><default></default></profiles>"
            "    <users>"
            "        <default>"
            "            <password></password>"
            "            <networks><ip>::/0</ip></networks>"
            "            <profile>default</profile>"
            "            <quota>default</quota>"
            "        </default>"
            "    </users>"
            "    <quotas><default></default></quotas>"
            "</clickhouse>";
        context->getAccessControl().setNoPasswordAllowed(true);
        context->setUsersConfig(make_config(minimal_users_xml));
        /// Resolve default_profile / system_profile (both default to "default") so SYSTEM and other
        /// queries can apply the system profile.
        context->setDefaultProfiles(context->getConfigRef());
        context->setUser(context->getAccessControl().getID<User>("default"));

        const std::string default_database = "default";
        if (!DatabaseCatalog::instance().isDatabaseExist(default_database))
        {
            DatabasePtr database = std::make_shared<DatabaseMemory>(default_database, context);
            DatabaseCatalog::instance().attachDatabase(default_database, database);
        }
        context->setCurrentDatabase(default_database);

        return true;
    }();
    (void)initialized;

    return context;
}

std::string runQueryAndReadResult(const std::string & sql, ContextMutablePtr global_context)
{
    /// Run each query in its own query-scoped context (a thread group + non-empty query id are
    /// required by ProcessList::insert / attachQueryForLog), exactly like the server/local does.
    auto query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    QueryScope query_scope = QueryScope::create(query_context);

    ReadBufferFromString in(sql);
    WriteBufferFromOwnString out;
    executeQuery(in, out, query_context, {}, QueryFlags{.internal = true});
    return out.str();
}

void runQuery(const std::string & sql, ContextMutablePtr global_context)
{
    runQueryAndReadResult(sql, global_context);
}

std::string runQueryAndGetError(const std::string & sql, ContextMutablePtr context)
{
    try
    {
        runQuery(sql, context);
    }
    catch (const Exception & e)
    {
        return e.message();
    }
    return {};
}

StorageMergeTree & getStorageMergeTree(const std::string & table, ContextMutablePtr context)
{
    auto storage = DatabaseCatalog::instance().getTable(StorageID("default", table), context);
    return dynamic_cast<StorageMergeTree &>(*storage);
}

}

/// End-to-end regression test for STID 4462-571d ("Part X intersects part Y"): the plain MergeTree
/// merge selector must not merge across a gap that a concurrently committing NewPart block is about
/// to fill. We make the gap real (DROP the middle part), register a committing block inside it via
/// a test hook, and drive a real OPTIMIZE FINAL through the actual selection path. Without the fix
/// the selector spans the gap and the merge later intersects the gap part; with the fix
/// canMergeParts refuses the span, so OPTIMIZE FINAL reports nothing to merge.
TEST(CommittingBlockGapOptimize, OptimizeFinalDoesNotMergeAcrossCommittingNewPart)
{
    auto context = setUpContext();

    const std::string table = "committing_gap_optimize";
    runQuery("DROP TABLE IF EXISTS default." + table + " SYNC", context);
    /// old_parts_lifetime = 0 so DROP PART's empty covering part (all_2_2_1, level 1) is removed
    /// immediately instead of lingering in the gap. Otherwise the pre-existing level-based gap check
    /// (getMaxLevelInBetween) would block the merge before our committing-block check is reached.
    runQuery(
        "CREATE TABLE default." + table
            + " (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS old_parts_lifetime = 0",
        context);

    /// Stop background merges so the three single-row parts (all_1_1_0, all_2_2_0, all_3_3_0) stay
    /// as is until we explicitly OPTIMIZE.
    runQuery("SYSTEM STOP MERGES default." + table, context);
    runQuery("INSERT INTO default." + table + " VALUES (1)", context);
    runQuery("INSERT INTO default." + table + " VALUES (2)", context);
    runQuery("INSERT INTO default." + table + " VALUES (3)", context);

    /// Punch a clean hole at block 2 so the live parts are all_1_1_0 and all_3_3_0 with an empty gap
    /// in between (no level > 0 outdated tombstone, thanks to old_parts_lifetime = 0).
    runQuery("ALTER TABLE default." + table + " DROP PART 'all_2_2_0'", context);
    runQuery("SYSTEM START MERGES default." + table, context);

    auto & storage = getStorageMergeTree(table, context);

    /// Pretend a part for block 2 is mid-commit inside the gap (this is what a concurrent
    /// MOVE/REPLACE/INSERT does in the real race). The holder keeps the block "committing".
    auto committing_gap = storage.injectCommittingBlockForTest(CommittingBlock(CommittingBlock::Op::NewPart, 2, "all"));

    /// Without the fix the selector spans the gap, merges, and later hits the "Part X intersects
    /// part Y" LOGICAL_ERROR; with the fix canMergeParts refuses the span and OPTIMIZE FINAL fails
    /// with optimize_throw_if_noop reporting the committing-block gap.
    const std::string error = runQueryAndGetError(
        "OPTIMIZE TABLE default." + table + " FINAL SETTINGS optimize_throw_if_noop = 1", context);
    EXPECT_NE(error.find("committing block in a gap"), std::string::npos)
        << "OPTIMIZE FINAL should refuse to merge across the committing gap, but got: " << error;

    /// Once the committing block is gone, the gap is genuinely empty and the merge proceeds.
    committing_gap.reset();
    runQuery("OPTIMIZE TABLE default." + table + " FINAL SETTINGS optimize_throw_if_noop = 1", context);

    /// The two active parts around the gap merged into a single part spanning blocks 1..3. Scope the
    /// parts vector so the part handles are released before DROP TABLE shuts the storage down (a
    /// part destructor reaches back into the storage caches).
    {
        const auto active_parts = storage.getDataPartsVectorForInternalUsage();
        ASSERT_EQ(active_parts.size(), 1u);
        EXPECT_EQ(active_parts.front()->name, "all_1_3_1");
    }

    runQuery("DROP TABLE default." + table + " SYNC", context);
}
