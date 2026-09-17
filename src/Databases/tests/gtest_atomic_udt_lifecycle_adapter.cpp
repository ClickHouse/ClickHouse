#include <Access/AccessControl.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/User.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/DefinitionMutationPlanner.h>
#include <Databases/UDT/ILifecycleAdapter.h>

#include <Disks/DiskLocal.h>
#include <Disks/tests/gtest_disk.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterUDTQuery.h>

#include <Parsers/ASTAlterTypeCommentQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Parsers/ASTRenameTypeQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

#include <base/scope_guard.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int UNKNOWN_TYPE;
extern const int UNFINISHED;
}

namespace DB::FailPoints
{
extern const char udt_authority_shutdown_pause_before_fence[];
extern const char udt_lifecycle_pause_after_database_lookup[];
}

namespace DB::UDT
{
namespace
{

class InjectedLifecycleDiskError final : public std::runtime_error
{
public:
    explicit InjectedLifecycleDiskError(std::string_view message)
        : std::runtime_error(String(message))
    {
    }
};

class ControllableLifecycleDisk final : public DiskLocal
{
public:
    using DiskLocal::DiskLocal;

    void armExistsProbe()
    {
        std::lock_guard lock(control_mutex);
        throw_on_exists = true;
        exists_calls = 0;
    }

    UInt64 disarmExistsProbe()
    {
        std::lock_guard lock(control_mutex);
        throw_on_exists = false;
        return exists_calls;
    }

    void failNextDirectorySyncContaining(String fragment)
    {
        std::lock_guard lock(control_mutex);
        sync_failure_fragment = std::move(fragment);
        sync_failure_matches_remaining = 1;
    }

    void failDirectorySyncContainingOnMatch(String fragment, UInt64 match)
    {
        if (match == 0)
            throw std::invalid_argument("directory-sync failure match must be nonzero");
        std::lock_guard lock(control_mutex);
        sync_failure_fragment = std::move(fragment);
        sync_failure_matches_remaining = match;
    }

    void failNextMoveToCanonicalTypeRecord()
    {
        std::lock_guard lock(control_mutex);
        fail_canonical_type_record_move = true;
    }

    void blockNextDirectorySyncContaining(String fragment)
    {
        std::lock_guard lock(control_mutex);
        blocked_sync_fragment = std::move(fragment);
        blocked_sync_reached = false;
        release_blocked_sync = false;
    }

    bool waitForBlockedDirectorySync(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(control_mutex);
        return control_cv.wait_for(lock, timeout, [&] { return blocked_sync_reached; });
    }

    void releaseBlockedDirectorySync()
    {
        std::lock_guard lock(control_mutex);
        release_blocked_sync = true;
        control_cv.notify_all();
    }

    bool existsFileOrDirectory(const String & path) const override
    {
        {
            std::lock_guard lock(control_mutex);
            if (throw_on_exists)
            {
                ++exists_calls;
                throw InjectedLifecycleDiskError("unexpected persistent-storage probe");
            }
        }
        return DiskLocal::existsFileOrDirectory(path);
    }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override
    {
        {
            std::unique_lock lock(control_mutex);
            if (!sync_failure_fragment.empty() && path.find(sync_failure_fragment) != String::npos)
            {
                if (--sync_failure_matches_remaining == 0)
                {
                    sync_failure_fragment.clear();
                    throw InjectedLifecycleDiskError("injected directory synchronization failure");
                }
            }
            if (!blocked_sync_fragment.empty() && path.find(blocked_sync_fragment) != String::npos)
            {
                blocked_sync_reached = true;
                control_cv.notify_all();
                control_cv.wait(lock, [&] { return release_blocked_sync; });
                blocked_sync_fragment.clear();
            }
        }
        return DiskLocal::getDirectorySyncGuard(path);
    }

    void moveFile(const String & from_path, const String & to_path) override
    {
        {
            std::lock_guard lock(control_mutex);
            if (fail_canonical_type_record_move && to_path.ends_with(".sql") && to_path.find("/types/") != String::npos)
            {
                fail_canonical_type_record_move = false;
                throw InjectedLifecycleDiskError("injected failure after durable Prepare");
            }
        }
        DiskLocal::moveFile(from_path, to_path);
    }

private:
    mutable std::mutex control_mutex;
    mutable std::condition_variable control_cv;
    mutable bool throw_on_exists = false;
    mutable UInt64 exists_calls = 0;
    mutable String sync_failure_fragment;
    mutable UInt64 sync_failure_matches_remaining = 0;
    mutable bool fail_canonical_type_record_move = false;
    mutable String blocked_sync_fragment;
    mutable bool blocked_sync_reached = false;
    mutable bool release_blocked_sync = false;
};

class LifecycleTestDatabase final : public DatabaseAtomic
{
public:
    LifecycleTestDatabase(String name, String metadata_root, UUID uuid, ContextPtr context_, DiskPtr disk)
        : DatabaseAtomic(std::move(name), std::move(metadata_root), uuid, "LifecycleTestDatabase", std::move(context_))
    {
        metadata_disk_ptr = std::move(disk);
    }

    const AtomicDatabaseSchemaMutationStorage * getMutationStorageForTest() const noexcept { return udt_mutation_storage.get(); }

    std::pair<UInt64, Digest> getTypeIndexStateForTest() const
    {
        std::lock_guard lock(udt_authority_mutex);
        if (!udt_authority)
            throw std::logic_error("test Atomic authority is absent");
        auto snapshot = udt_authority->acquireCurrentRoot();
        if (!snapshot)
            throw std::logic_error("test Atomic authority root is absent");
        return {snapshot->getTypeIndexGeneration(), snapshot->getTypeIndexContentDigest()};
    }

    AuthorityState getAuthorityStateForTest() const
    {
        std::lock_guard lock(udt_authority_mutex);
        if (!udt_authority)
            throw std::logic_error("test Atomic authority is absent");
        auto snapshot = udt_authority->acquireCurrentRoot();
        if (!snapshot)
            throw std::logic_error("test Atomic authority root is absent");
        return snapshot->getAuthorityState();
    }

    bool tryAcquireUDTSchemaMutationLockForTest()
    {
        std::unique_lock lock(udt_schema_mutation_mutex, std::try_to_lock);
        return lock.owns_lock();
    }

    bool hasDegradedUDTStartupStatusForTest() const
    {
        std::lock_guard lock(udt_authority_mutex);
        return static_cast<bool>(udt_degraded_startup_status);
    }

    CrossDatabaseMoveGuard acquireUDTCrossDatabaseTargetGuardForTest(UUID incoming_table_uuid, std::string_view table_name_for_logs)
    {
        return acquireUDTCrossDatabaseTargetGuard(incoming_table_uuid, table_name_for_logs);
    }
};

UUID testUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

ASTPtr parseTypeQuery(const String & text)
{
    ParserQuery parser(text.data() + text.size());
    return parseQuery(parser, text, "Atomic user-defined type lifecycle test", 0, 256, 1'000'000);
}

bool waitForFailPointPause(const String & fail_point_name, std::chrono::milliseconds timeout)
{
    std::packaged_task<void()> wait_task([&] { FailPointInjection::waitForPause(fail_point_name); });
    auto wait_result = wait_task.get_future();
    std::thread wait_thread(std::move(wait_task));
    SCOPE_EXIT({
        if (wait_thread.joinable())
            wait_thread.join();
    });

    if (wait_result.wait_for(timeout) != std::future_status::ready)
    {
        /// Wake both the waiter and a target that may have reached the failpoint
        /// concurrently with the timeout.
        FailPointInjection::disableFailPoint(fail_point_name);
        return false;
    }

    wait_thread.join();
    wait_result.get();
    return true;
}

class AtomicLifecycleAdapterTest : public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        auto & catalog = DatabaseCatalog::instance();
        ASSERT_FALSE(catalog.hasUUIDMapping(databaseUUID()));
        catalog.addUUIDMapping(databaseUUID());

        auto & access_control = getMutableContext().context->getAccessControl();
        test_access_storage
            = std::make_shared<MemoryAccessStorage>(String(access_storage_name), access_control.getChangesNotifier(), false);
        access_control.addStorage(test_access_storage);
        auto user = std::make_shared<User>();
        user->setName(String(test_user_name));
        user->access.grant(AccessType::CREATE_TYPE, database_name);
        ASSERT_TRUE(test_access_storage->insert(testUserUUID(), user, false, true));
    }

    static void TearDownTestSuite()
    {
        auto & access_control = getMutableContext().context->getAccessControl();
        if (test_access_storage)
        {
            EXPECT_TRUE(test_access_storage->remove(testUserUUID(), false));
            access_control.removeStorage(test_access_storage);
            test_access_storage.reset();
        }
        else
        {
            ADD_FAILURE() << "Lifecycle test access storage is missing";
        }

        auto & catalog = DatabaseCatalog::instance();
        if (catalog.hasUUIDMapping(databaseUUID()))
        {
            const auto [mapped_database, mapped_table] = catalog.tryGetByUUID(databaseUUID());
            EXPECT_EQ(mapped_database, nullptr);
            EXPECT_EQ(mapped_table, nullptr);
            if (!mapped_database && !mapped_table)
                catalog.removeUUIDMappingFinally(databaseUUID());
        }
        else
        {
            ADD_FAILURE() << "Lifecycle test database UUID reservation is missing";
        }
    }

    void SetUp() override
    {
        DiskPtr seed = createDisk("atomic_udt_lifecycle");
        const String disk_path = seed->getPath();
        seed.reset();
        controlled_disk = std::make_shared<ControllableLifecycleDisk>("lifecycle_disk", disk_path);
        disk = controlled_disk;
        context = Context::createCopy(getContext().context);
        createDatabase();
    }

    void TearDown() override
    {
        if (database_attached_to_catalog)
        {
            try
            {
                DatabaseCatalog::instance().detachDatabase(context, String(database_name), false, false);
                database_attached_to_catalog = false;
            }
            catch (...)
            {
                ADD_FAILURE() << "Failed to detach the lifecycle test database from DatabaseCatalog";
            }
        }
        database.reset();
        controlled_disk.reset();
        destroyDisk(disk);
    }

protected:
    static constexpr std::string_view database_name = "udt_lifecycle";
    static constexpr std::string_view metadata_root = "metadata/udt_lifecycle";
    static constexpr std::string_view access_storage_name = "udt_lifecycle_test_access";
    static constexpr std::string_view test_user_name = "lifecycle-owner";

    static UUID databaseUUID() { return testUUID(0x1020304050607080ULL, 0x90a0b0c0d0e0f001ULL); }
    static UUID testUserUUID() { return testUUID(0x9000, 1); }

    inline static std::shared_ptr<MemoryAccessStorage> test_access_storage;

    void createDatabase()
    {
        database = std::make_shared<LifecycleTestDatabase>(String(database_name), String(metadata_root), database_uuid, context, disk);
    }

    void restartDatabase()
    {
        ASSERT_FALSE(database_attached_to_catalog);
        database.reset();
        createDatabase();
        database->beforeLoadingMetadata(context, LoadingStrictnessLevel::ATTACH);
    }

    void attachDatabaseToCatalog()
    {
        ASSERT_FALSE(database_attached_to_catalog);
        DatabaseCatalog::instance().attachDatabase(String(database_name), database);
        database_attached_to_catalog = true;
    }

    ILifecycleAdapter & lifecycle() { return database->getUDTLifecycleAdapter(); }

    void createOrAttach(const String & text, LifecycleActor mutation_actor = {})
    {
        if (mutation_actor.principal_uuid == UUIDHelpers::Nil)
            mutation_actor = actor;
        ASTPtr ast = parseTypeQuery(text);
        lifecycle().createOrAttach(ast->as<ASTCreateTypeQuery &>(), mutation_actor);
    }

    void rename(const String & text)
    {
        ASTPtr ast = parseTypeQuery(text);
        lifecycle().rename(ast->as<ASTRenameTypeQuery &>(), actor);
    }

    void comment(const String & text)
    {
        ASTPtr ast = parseTypeQuery(text);
        lifecycle().comment(ast->as<ASTAlterTypeCommentQuery &>(), actor);
    }

    void drop(const String & text)
    {
        ASTPtr ast = parseTypeQuery(text);
        lifecycle().dropRestrict(ast->as<ASTDropTypeQuery &>(), actor);
    }

    Record record(std::string_view local_name)
    {
        auto snapshot = lifecycle().acquireSnapshot();
        const auto * result = snapshot->findDefinitionRecordByLocalName(local_name);
        if (!result)
            throw std::logic_error("test definition record is absent");
        return *result;
    }

    const UUID database_uuid = databaseUUID();
    const LifecycleActor actor{
        .principal_uuid = testUserUUID(),
        .principal_display_name = String(test_user_name),
        .internal_query = false,
    };
    ContextMutablePtr context;
    DiskPtr disk;
    std::shared_ptr<ControllableLifecycleDisk> controlled_disk;
    std::shared_ptr<LifecycleTestDatabase> database;
    bool database_attached_to_catalog = false;
};

TEST_F(AtomicLifecycleAdapterTest, FirstCreateIsDurableBeforeExactDefinitionOnlyActivation)
{
    EXPECT_TRUE(database->empty());
    auto empty = lifecycle().acquireSnapshot();
    EXPECT_EQ(empty->getDatabaseCatalogEpoch(), 0);
    EXPECT_TRUE(empty->getDefinitionRecords().empty());
    empty.reset();
    EXPECT_EQ(database->getMutationStorageForTest(), nullptr);
    EXPECT_FALSE(disk->existsFileOrDirectory(String(metadata_root) + "/types"));

    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS BIGINT");

    EXPECT_FALSE(database->empty());
    EXPECT_TRUE(database->hasActiveUDTAuthority());
    auto snapshot = lifecycle().acquireSnapshot();
    ASSERT_EQ(snapshot->getDatabaseCatalogEpoch(), 1);
    ASSERT_EQ(snapshot->getDefinitionRecords().size(), 1);
    const auto & created = snapshot->getDefinitionRecords().front();
    EXPECT_EQ(created.normalized_name, "udt_lifecycle.Alpha");
    EXPECT_EQ(created.canonical_physical_template_sql, "Int64");
    EXPECT_TRUE(created.canonical_definition_sql.starts_with("ATTACH TYPE udt_lifecycle.Alpha"));
    const auto projection = snapshot->getMonomorphicProjection(created.identity);
    ASSERT_TRUE(projection);
    EXPECT_EQ(projection->canonical_physical_type, "Int64");

    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    ASSERT_TRUE(storage->hasDurableAuthorityMarker());
    EXPECT_EQ(storage->getDurableHighWaterMark(), 1);
    const auto state = storage->getCurrentAuthorityState();
    ASSERT_TRUE(state);
    EXPECT_EQ(state->database_catalog_epoch, 1);
    EXPECT_EQ(state->persistent_capability_mask, definition_authority_capability_mask);
    EXPECT_TRUE(disk->existsFile(storage->getPaths().activationMarkerPath()));
    EXPECT_TRUE(disk->existsFile(storage->getPaths().commitPath(1)));
}

TEST_F(AtomicLifecycleAdapterTest, CrossDatabaseTargetGuardHoldsSchemaMutationLockUntilDestroyed)
{
    const auto try_lock_from_another_thread
        = [&] { return std::async(std::launch::async, [&] { return database->tryAcquireUDTSchemaMutationLockForTest(); }).get(); };

    std::optional<DatabaseAtomic::CrossDatabaseMoveGuard> target_guard;
    target_guard.emplace(database->acquireUDTCrossDatabaseTargetGuardForTest(testUUID(0x5000, 1), "ordinary.events"));
    EXPECT_FALSE(try_lock_from_another_thread());

    target_guard.reset();
    EXPECT_TRUE(try_lock_from_another_thread());
}

TEST_F(AtomicLifecycleAdapterTest, StartupDependentObjectActivationPublishesOnlyAfterDurableCommitAndIsRestartIdempotent)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64");
    const auto * initial_storage = database->getMutationStorageForTest();
    ASSERT_NE(initial_storage, nullptr);
    const String activation_staging_path = initial_storage->getPaths().stagingTransactionDirectory(2);

    database.reset();
    createDatabase();
    controlled_disk->failNextDirectorySyncContaining(activation_staging_path);
    EXPECT_THROW(database->beforeLoadingMetadata(context, LoadingStrictnessLevel::ATTACH), InjectedLifecycleDiskError);
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_EQ(database->getMutationStorageForTest(), nullptr);

    restartDatabase();
    ASSERT_TRUE(database->hasActiveUDTAuthority());
    const auto activated_state = database->getAuthorityStateForTest();
    EXPECT_EQ(activated_state.database_catalog_epoch, 2);
    EXPECT_EQ(activated_state.persistent_capability_mask, dependent_object_authority_capability_mask);
    const auto * activated_storage = database->getMutationStorageForTest();
    ASSERT_NE(activated_storage, nullptr);
    EXPECT_EQ(activated_storage->getDurableHighWaterMark(), 2);
    const auto activated_transactions = activated_storage->listDurableTransactionIDs();

    restartDatabase();
    ASSERT_TRUE(database->hasActiveUDTAuthority());
    EXPECT_EQ(database->getAuthorityStateForTest(), activated_state);
    const auto * restarted_storage = database->getMutationStorageForTest();
    ASSERT_NE(restarted_storage, nullptr);
    EXPECT_EQ(restarted_storage->getDurableHighWaterMark(), 2);
    EXPECT_EQ(restarted_storage->listDurableTransactionIDs(), activated_transactions);
}

TEST_F(AtomicLifecycleAdapterTest, RuntimeDependentObjectAdmissionIsExplicitAndIdempotent)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64");
    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    const auto definition_only_type_index = database->getTypeIndexStateForTest();
    auto pre_activation_snapshot = lifecycle().acquireSnapshot();
    ASSERT_EQ(pre_activation_snapshot->getDatabaseCatalogEpoch(), 1);
    EXPECT_EQ(database->getAuthorityStateForTest().persistent_capability_mask, definition_authority_capability_mask);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 1);

    database->ensureUDTDependentObjectCapabilities();

    /// Activation is content-neutral but epoch-changing. A dependent-object admission caller
    /// must ensure activation first and only then pin, bind, and plan against epoch 2.
    EXPECT_EQ(pre_activation_snapshot->getDatabaseCatalogEpoch(), 1);
    auto post_activation_snapshot = lifecycle().acquireSnapshot();
    EXPECT_EQ(post_activation_snapshot->getDatabaseCatalogEpoch(), 2);
    const auto dependent_object_state = database->getAuthorityStateForTest();
    EXPECT_EQ(dependent_object_state.database_catalog_epoch, 2);
    EXPECT_EQ(dependent_object_state.persistent_capability_mask, dependent_object_authority_capability_mask);
    EXPECT_EQ(database->getTypeIndexStateForTest(), definition_only_type_index);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 2);
    const auto activated_transactions = storage->listDurableTransactionIDs();

    database->ensureUDTDependentObjectCapabilities();
    EXPECT_EQ(database->getAuthorityStateForTest(), dependent_object_state);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 2);
    EXPECT_EQ(storage->listDurableTransactionIDs(), activated_transactions);
}

TEST_F(AtomicLifecycleAdapterTest, RuntimeDependentObjectAdmissionRequiresAnAlreadyPublishedDefinitionOnlyRoot)
{
    controlled_disk->armExistsProbe();
    try
    {
        database->ensureUDTDependentObjectCapabilities();
        FAIL() << "dependent-object authority admission unexpectedly succeeded without a published definition-only authority authority";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::UNKNOWN_TYPE) << error.message();
    }
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_FALSE(disk->existsFileOrDirectory(String(metadata_root) + "/types"));
}

TEST_F(AtomicLifecycleAdapterTest, RuntimeDependentObjectAdmissionRetriesAfterPrePrepareFailure)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64");
    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    controlled_disk->failNextDirectorySyncContaining(storage->getPaths().stagingTransactionDirectory(2));

    EXPECT_THROW(database->ensureUDTDependentObjectCapabilities(), InjectedLifecycleDiskError);
    EXPECT_EQ(database->getAuthorityStateForTest().persistent_capability_mask, definition_authority_capability_mask);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 1);
    EXPECT_FALSE(storage->getRecoveryRequiredTransactionID());

    EXPECT_NO_THROW(database->ensureUDTDependentObjectCapabilities());
    EXPECT_EQ(database->getAuthorityStateForTest().persistent_capability_mask, dependent_object_authority_capability_mask);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 2);
}

TEST_F(AtomicLifecycleAdapterTest, RuntimeDependentObjectAdmissionRollsBackIndeterminatePrepareAndRetries)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64");
    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    controlled_disk->failNextDirectorySyncContaining(storage->getPaths().walTransactionDirectory(2));

    EXPECT_THROW(database->ensureUDTDependentObjectCapabilities(), DatabaseSchemaMutationIndeterminateDurabilityError);
    EXPECT_EQ(database->getAuthorityStateForTest().persistent_capability_mask, definition_authority_capability_mask);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 2);
    EXPECT_FALSE(storage->getRecoveryRequiredTransactionID());

    EXPECT_NO_THROW(database->ensureUDTDependentObjectCapabilities());
    const auto dependent_object_state = database->getAuthorityStateForTest();
    EXPECT_EQ(dependent_object_state.database_catalog_epoch, 2);
    EXPECT_EQ(dependent_object_state.persistent_capability_mask, dependent_object_authority_capability_mask);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 3);
}

TEST_F(AtomicLifecycleAdapterTest, RuntimeDependentObjectAdmissionPublishesARecoveredIndeterminateCommit)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64");
    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    controlled_disk->failDirectorySyncContainingOnMatch(storage->getPaths().walTransactionDirectory(2), 2);

    EXPECT_NO_THROW(database->ensureUDTDependentObjectCapabilities());
    const auto dependent_object_state = database->getAuthorityStateForTest();
    EXPECT_EQ(dependent_object_state.database_catalog_epoch, 2);
    EXPECT_EQ(dependent_object_state.persistent_capability_mask, dependent_object_authority_capability_mask);
    EXPECT_EQ(storage->getDurableHighWaterMark(), 2);
    EXPECT_TRUE(disk->existsFile(storage->getPaths().commitPath(2)));
    EXPECT_TRUE(disk->existsFile(storage->getPaths().recoveryDecisionPath(2)));
    EXPECT_FALSE(storage->getRecoveryRequiredTransactionID());
}

TEST_F(AtomicLifecycleAdapterTest, RuntimeDependentObjectAdmissionFailsClosedOnAnUnrelatedPreparedMutation)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64");
    controlled_disk->failNextMoveToCanonicalTypeRecord();
    EXPECT_THROW(createOrAttach("CREATE TYPE udt_lifecycle.Pending AS UInt32"), DatabaseSchemaMutationIndeterminateDurabilityError);

    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    ASSERT_EQ(storage->getRecoveryRequiredTransactionID(), 2);
    EXPECT_THROW(database->ensureUDTDependentObjectCapabilities(), DatabaseSchemaMutationReplayConflictError);
    EXPECT_EQ(database->getAuthorityStateForTest().persistent_capability_mask, definition_authority_capability_mask);
    EXPECT_EQ(storage->getRecoveryRequiredTransactionID(), 2);
    EXPECT_FALSE(disk->existsFile(storage->getPaths().commitPath(2)));
}

TEST_F(AtomicLifecycleAdapterTest, TypeOnlyDatabaseEmptinessTracksDurableDefinitions)
{
    EXPECT_TRUE(database->empty());
    createOrAttach("CREATE TYPE udt_lifecycle.OnlyObject AS UInt64");
    EXPECT_FALSE(database->empty());

    restartDatabase();
    EXPECT_FALSE(database->empty());

    drop("DROP TYPE udt_lifecycle.OnlyObject RESTRICT");
    EXPECT_TRUE(database->empty());

    restartDatabase();
    EXPECT_TRUE(database->empty());
}

TEST_F(AtomicLifecycleAdapterTest, StartupDegradesAfterWholeTypesTreeDeletionUsingDatabaseRootMarker)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64");
    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    const String types_path = storage->getPaths().typesDirectory();
    const String activation_marker_path = storage->getPaths().activationMarkerPath();
    ASSERT_TRUE(disk->existsFile(activation_marker_path));

    database.reset();
    disk->removeRecursive(types_path);
    ASSERT_TRUE(disk->existsFile(activation_marker_path));
    createDatabase();
    EXPECT_NO_THROW(database->beforeLoadingMetadata(context, LoadingStrictnessLevel::ATTACH));
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_TRUE(database->hasDegradedUDTStartupStatusForTest());
    auto snapshot = lifecycle().acquireSnapshot();
    EXPECT_EQ(snapshot->getDatabaseCatalogEpoch(), 0);
    EXPECT_TRUE(snapshot->getDefinitionRecords().empty());
    EXPECT_TRUE(disk->existsFile(activation_marker_path));
    EXPECT_FALSE(disk->existsFileOrDirectory(types_path));
}

TEST_F(AtomicLifecycleAdapterTest, ProvenCreateIfNotExistsPerformsNoStorageIO)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS BIGINT");
    createOrAttach("CREATE TYPE udt_lifecycle.Caller AS Array(udt_lifecycle.Alpha)");
    const UInt64 epoch = lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch();

    controlled_disk->armExistsProbe();
    EXPECT_NO_THROW(createOrAttach("CREATE TYPE IF NOT EXISTS udt_lifecycle.Alpha AS BIGINT"));
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);
    EXPECT_EQ(lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch(), epoch);

    controlled_disk->armExistsProbe();
    try
    {
        createOrAttach("CREATE TYPE IF NOT EXISTS udt_lifecycle.Alpha AS UInt32");
        FAIL() << "mismatching CREATE IF NOT EXISTS unexpectedly became a no-op";
    }
    catch (const DefinitionMutationPlannerError & error)
    {
        EXPECT_EQ(error.code, DefinitionMutationPlannerError::Code::DefinitionConflict);
    }
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);
    EXPECT_EQ(lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch(), epoch);
}

TEST_F(AtomicLifecycleAdapterTest, InternalAttachValidatesExactUUIDHashAndNoOpIdentity)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Restored AS UInt32 COMMENT 'kept'");
    const auto durable_record = record("Restored");
    drop("DROP TYPE udt_lifecycle.Restored RESTRICT");

    ASTPtr wrong_hash = parseTypeQuery(durable_record.canonical_definition_sql);
    wrong_hash->as<ASTCreateTypeQuery &>().definition_hash = String(64, '0');
    auto internal_actor = actor;
    internal_actor.internal_query = true;
    EXPECT_THROW(lifecycle().createOrAttach(wrong_hash->as<ASTCreateTypeQuery &>(), internal_actor), Exception);
    EXPECT_EQ(lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch(), 2);

    createOrAttach(durable_record.canonical_definition_sql, internal_actor);
    const auto restored = record("Restored");
    EXPECT_EQ(restored.identity, durable_record.identity);
    EXPECT_EQ(restored.definition_hash, durable_record.definition_hash);
    createOrAttach("CREATE TYPE udt_lifecycle.RestoredCaller AS Array(udt_lifecycle.Restored)");

    ASTPtr exact_no_op = parseTypeQuery(durable_record.canonical_definition_sql);
    exact_no_op->as<ASTCreateTypeQuery &>().if_not_exists = true;
    controlled_disk->armExistsProbe();
    EXPECT_NO_THROW(lifecycle().createOrAttach(exact_no_op->as<ASTCreateTypeQuery &>(), internal_actor));
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);

    ASTPtr wrong_uuid = exact_no_op->clone();
    wrong_uuid->as<ASTCreateTypeQuery &>().uuid = testUUID(0x8000, 2);
    controlled_disk->armExistsProbe();
    try
    {
        lifecycle().createOrAttach(wrong_uuid->as<ASTCreateTypeQuery &>(), internal_actor);
        FAIL() << "ATTACH IF NOT EXISTS accepted a different stable UUID";
    }
    catch (const DefinitionMutationPlannerError & error)
    {
        EXPECT_EQ(error.code, DefinitionMutationPlannerError::Code::DefinitionConflict);
    }
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);

    ASTPtr wrong_revision = exact_no_op->clone();
    wrong_revision->as<ASTCreateTypeQuery &>().revision = durable_record.identity.revision + 1;
    controlled_disk->armExistsProbe();
    EXPECT_THROW(lifecycle().createOrAttach(wrong_revision->as<ASTCreateTypeQuery &>(), internal_actor), Exception);
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);
}

TEST_F(AtomicLifecycleAdapterTest, RenameRewritesExactDirectDependentsAndRecoversDiamond)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Base AS UInt64");
    createOrAttach("CREATE TYPE udt_lifecycle.Left AS Array(udt_lifecycle.Base)");
    createOrAttach("CREATE TYPE udt_lifecycle.Right AS Nullable(udt_lifecycle.Base)");
    createOrAttach("CREATE TYPE udt_lifecycle.Diamond AS Tuple(udt_lifecycle.Left, udt_lifecycle.Right)");

    const auto base_before = record("Base");
    const auto left_before = record("Left");
    const auto right_before = record("Right");
    const auto diamond_before = record("Diamond");

    rename("ALTER TYPE udt_lifecycle.Base RENAME TO Renamed");

    const auto base_after = record("Renamed");
    const auto left_after = record("Left");
    const auto right_after = record("Right");
    const auto diamond_after = record("Diamond");
    EXPECT_EQ(base_after.identity, base_before.identity);
    EXPECT_EQ(base_after.definition_hash, base_before.definition_hash);
    EXPECT_EQ(left_after.identity, left_before.identity);
    EXPECT_EQ(left_after.definition_hash, left_before.definition_hash);
    EXPECT_EQ(right_after.identity, right_before.identity);
    EXPECT_EQ(right_after.definition_hash, right_before.definition_hash);
    EXPECT_NE(left_after.canonical_definition_sql, left_before.canonical_definition_sql);
    EXPECT_NE(left_after.canonical_physical_template_sql, left_before.canonical_physical_template_sql);
    EXPECT_NE(right_after.canonical_definition_sql, right_before.canonical_definition_sql);
    EXPECT_NE(right_after.canonical_physical_template_sql, right_before.canonical_physical_template_sql);
    EXPECT_NE(left_after.canonical_physical_template_sql.find("udt_lifecycle.Renamed"), String::npos);
    EXPECT_NE(right_after.canonical_physical_template_sql.find("udt_lifecycle.Renamed"), String::npos);
    EXPECT_EQ(diamond_after, diamond_before);

    restartDatabase();
    EXPECT_TRUE(database->hasActiveUDTAuthority());
    auto recovered = lifecycle().acquireSnapshot();
    EXPECT_EQ(recovered->getDatabaseCatalogEpoch(), 6);
    EXPECT_EQ(database->getAuthorityStateForTest().persistent_capability_mask, dependent_object_authority_capability_mask);
    EXPECT_EQ(recovered->getDefinitionRecords().size(), 4);
    const auto * recovered_base = recovered->findDefinitionRecordByLocalName("Renamed");
    const auto * recovered_left = recovered->findDefinitionRecordByLocalName("Left");
    const auto * recovered_right = recovered->findDefinitionRecordByLocalName("Right");
    const auto * recovered_diamond = recovered->findDefinitionRecordByLocalName("Diamond");
    ASSERT_NE(recovered_base, nullptr);
    ASSERT_NE(recovered_left, nullptr);
    ASSERT_NE(recovered_right, nullptr);
    ASSERT_NE(recovered_diamond, nullptr);
    EXPECT_EQ(*recovered_base, base_after);
    EXPECT_EQ(*recovered_left, left_after);
    EXPECT_EQ(*recovered_right, right_after);
    EXPECT_EQ(*recovered_diamond, diamond_after);
}

TEST_F(AtomicLifecycleAdapterTest, SelfRecursiveRenameRecoversAndDropIgnoresOnlyItsSelfEdge)
{
    createOrAttach(
        "CREATE TYPE udt_lifecycle.Tree(T TYPE, N UInt16) DECREASES N "
        "AS TYPE_IF(N = 0, T, udt_lifecycle.Tree(T, N - 1))");
    const auto tree = record("Tree");

    rename("ALTER TYPE udt_lifecycle.Tree RENAME TO Node");
    const auto node = record("Node");
    EXPECT_EQ(node.identity, tree.identity);
    EXPECT_EQ(node.definition_hash, tree.definition_hash);
    EXPECT_NE(node.canonical_physical_template_sql.find("udt_lifecycle.Node"), String::npos);

    restartDatabase();
    EXPECT_NE(record("Node").canonical_physical_template_sql.find("udt_lifecycle.Node"), String::npos);
    EXPECT_NO_THROW(drop("DROP TYPE udt_lifecycle.Node RESTRICT"));
    EXPECT_TRUE(lifecycle().acquireSnapshot()->getDefinitionRecords().empty());
    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    EXPECT_TRUE(disk->existsFile(storage->getPaths().activationMarkerPath()));
    restartDatabase();
    EXPECT_TRUE(database->hasActiveUDTAuthority());
    EXPECT_TRUE(lifecycle().acquireSnapshot()->getDefinitionRecords().empty());
}

TEST_F(AtomicLifecycleAdapterTest, CommentIsDurableAndDoesNotChangeExecutableIdentity)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Alpha AS UInt64 COMMENT 'before'");
    const auto before = record("Alpha");
    const UInt64 before_epoch = lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch();
    const auto before_type_index = database->getTypeIndexStateForTest();

    comment("ALTER TYPE udt_lifecycle.Alpha COMMENT 'after'");
    const auto after = record("Alpha");
    EXPECT_EQ(after.identity, before.identity);
    EXPECT_EQ(after.definition_hash, before.definition_hash);
    EXPECT_EQ(after.canonical_physical_template_sql, before.canonical_physical_template_sql);
    EXPECT_EQ(after.comment, "after");
    EXPECT_NE(after.canonical_definition_sql, before.canonical_definition_sql);
    EXPECT_EQ(lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch(), before_epoch + 1);
    EXPECT_EQ(database->getTypeIndexStateForTest(), before_type_index);

    const UInt64 material_epoch = lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch();
    const auto material_type_index = database->getTypeIndexStateForTest();
    controlled_disk->armExistsProbe();
    EXPECT_NO_THROW(comment("ALTER TYPE udt_lifecycle.Alpha COMMENT 'after'"));
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);
    EXPECT_EQ(lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch(), material_epoch);
    EXPECT_EQ(database->getTypeIndexStateForTest(), material_type_index);

    comment("ALTER TYPE udt_lifecycle.Alpha COMMENT ''");
    EXPECT_TRUE(record("Alpha").comment.empty());
    EXPECT_EQ(record("Alpha").canonical_definition_sql.find(" COMMENT "), String::npos);

    restartDatabase();
    const auto recovered = record("Alpha");
    EXPECT_EQ(recovered.identity, before.identity);
    EXPECT_EQ(recovered.definition_hash, before.definition_hash);
    EXPECT_TRUE(recovered.comment.empty());

    const UInt64 recovered_epoch = lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch();
    const auto recovered_type_index = database->getTypeIndexStateForTest();
    controlled_disk->armExistsProbe();
    EXPECT_NO_THROW(comment("ALTER TYPE IF EXISTS udt_lifecycle.Missing COMMENT 'ignored'"));
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);
    EXPECT_EQ(lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch(), recovered_epoch);
    EXPECT_EQ(database->getTypeIndexStateForTest(), recovered_type_index);
}

TEST_F(AtomicLifecycleAdapterTest, DropRestrictRejectsEveryExternalDependentAndIfExistsIsExact)
{
    createOrAttach("CREATE TYPE udt_lifecycle.Leaf AS UInt64");
    createOrAttach("CREATE TYPE udt_lifecycle.Caller AS Array(udt_lifecycle.Leaf)");
    const UInt64 blocked_epoch = lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch();

    try
    {
        drop("DROP TYPE udt_lifecycle.Leaf RESTRICT");
        FAIL() << "DROP RESTRICT unexpectedly ignored an external dependent";
    }
    catch (const DefinitionMutationPlannerError & error)
    {
        EXPECT_EQ(error.code, DefinitionMutationPlannerError::Code::ReferencedDefinition);
    }
    EXPECT_EQ(lifecycle().acquireSnapshot()->getDatabaseCatalogEpoch(), blocked_epoch);

    drop("DROP TYPE udt_lifecycle.Caller RESTRICT");
    drop("DROP TYPE udt_lifecycle.Leaf RESTRICT");
    controlled_disk->armExistsProbe();
    EXPECT_NO_THROW(drop("DROP TYPE IF EXISTS udt_lifecycle.Leaf RESTRICT"));
    EXPECT_EQ(controlled_disk->disarmExistsProbe(), 0);
}

TEST_F(AtomicLifecycleAdapterTest, FailureBeforePrepareCleansOnlyTheNeverEnabledScaffold)
{
    controlled_disk->failNextDirectorySyncContaining("/staging");
    EXPECT_THROW(createOrAttach("CREATE TYPE udt_lifecycle.EarlyFailure AS UInt64"), InjectedLifecycleDiskError);

    EXPECT_FALSE(database->hasActiveUDTAuthority());
    auto snapshot = lifecycle().acquireSnapshot();
    EXPECT_EQ(snapshot->getDatabaseCatalogEpoch(), 0);
    EXPECT_TRUE(snapshot->getDefinitionRecords().empty());
    snapshot.reset();
    EXPECT_FALSE(disk->existsFileOrDirectory(String(metadata_root) + "/types"));

    EXPECT_NO_THROW(createOrAttach("CREATE TYPE udt_lifecycle.Retry AS UInt64"));
    EXPECT_TRUE(database->hasActiveUDTAuthority());
}

TEST_F(AtomicLifecycleAdapterTest, FailureAfterPrepareFailStopsWithoutPublication)
{
    controlled_disk->failNextMoveToCanonicalTypeRecord();
    EXPECT_THROW(createOrAttach("CREATE TYPE udt_lifecycle.Ambiguous AS UInt64"), DatabaseSchemaMutationIndeterminateDurabilityError);

    EXPECT_FALSE(database->hasActiveUDTAuthority());
    auto snapshot = lifecycle().acquireSnapshot();
    EXPECT_EQ(snapshot->getDatabaseCatalogEpoch(), 0);
    EXPECT_TRUE(snapshot->getDefinitionRecords().empty());

    const auto * storage = database->getMutationStorageForTest();
    ASSERT_NE(storage, nullptr);
    EXPECT_TRUE(disk->existsFile(storage->getPaths().preparePath(1)));
    EXPECT_FALSE(disk->existsFile(storage->getPaths().commitPath(1)));
    try
    {
        createOrAttach("CREATE TYPE udt_lifecycle.StillBlocked AS UInt64");
        FAIL() << "a second first-activation attempt crossed indeterminate durable state";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::ABORTED);
        EXPECT_NE(error.message().find("no longer has the exact never-enabled image"), String::npos) << error.message();
    }
    EXPECT_FALSE(database->hasActiveUDTAuthority());
}

TEST_F(AtomicLifecycleAdapterTest, InterpreterMutationSerializesWithDatabaseLevelDDL)
{
    attachDatabaseToCatalog();
    context->setUser(testUserUUID());
    context->setCurrentDatabase(String(database_name));
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{1}});

    FailPointInjection::enableFailPoint(FailPoints::udt_lifecycle_pause_after_database_lookup);
    std::exception_ptr mutation_error;
    std::thread mutation_thread;
    SCOPE_EXIT({
        controlled_disk->releaseBlockedDirectorySync();
        FailPointInjection::disableFailPoint(FailPoints::udt_lifecycle_pause_after_database_lookup);
        if (mutation_thread.joinable())
            mutation_thread.join();
    });
    mutation_thread = std::thread(
        [&]
        {
            try
            {
                InterpreterUDTQuery interpreter(parseTypeQuery("CREATE TYPE udt_lifecycle.Guarded AS UInt64"), context);
                static_cast<void>(interpreter.execute());
            }
            catch (...)
            {
                mutation_error = std::current_exception();
            }
        });

    ASSERT_TRUE(waitForFailPointPause(FailPoints::udt_lifecycle_pause_after_database_lookup, std::chrono::seconds(10)));

    /// The interpreter has resolved its DatabasePtr but cannot request the DDL
    /// guard until the failpoint is resumed. Acquire the same guard first to
    /// deterministically model DROP/DETACH DATABASE winning that boundary.
    auto database_ddl_guard = DatabaseCatalog::instance().getDDLGuard(String(database_name), "", database.get());
    controlled_disk->blockNextDirectorySyncContaining("/staging");
    FailPointInjection::notifyFailPoint(FailPoints::udt_lifecycle_pause_after_database_lookup);

    const bool mutation_entered_while_database_ddl_was_held = controlled_disk->waitForBlockedDirectorySync(std::chrono::milliseconds(500));
    database_ddl_guard.reset();
    const bool mutation_entered_after_database_ddl_was_released = controlled_disk->waitForBlockedDirectorySync(std::chrono::seconds(10));

    controlled_disk->releaseBlockedDirectorySync();
    mutation_thread.join();

    EXPECT_FALSE(mutation_entered_while_database_ddl_was_held);
    EXPECT_TRUE(mutation_entered_after_database_ddl_was_released);
    EXPECT_EQ(mutation_error, nullptr);
    if (!mutation_error)
        EXPECT_EQ(record("Guarded").normalized_name, "udt_lifecycle.Guarded");
}

TEST_F(AtomicLifecycleAdapterTest, InterpreterRejectsDatabaseDetachedBetweenLookupAndDDLGuard)
{
    attachDatabaseToCatalog();
    context->setCurrentDatabase(String(database_name));
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{1}});

    FailPointInjection::enableFailPoint(FailPoints::udt_lifecycle_pause_after_database_lookup);
    std::exception_ptr mutation_error;
    std::thread mutation_thread;
    SCOPE_EXIT({
        FailPointInjection::disableFailPoint(FailPoints::udt_lifecycle_pause_after_database_lookup);
        if (mutation_thread.joinable())
            mutation_thread.join();
    });
    mutation_thread = std::thread(
        [&]
        {
            try
            {
                InterpreterUDTQuery interpreter(parseTypeQuery("CREATE TYPE udt_lifecycle.Stale AS UInt64"), context);
                static_cast<void>(interpreter.execute());
            }
            catch (...)
            {
                mutation_error = std::current_exception();
            }
        });

    /// Pause after the interpreter resolved its DatabasePtr, then execute the
    /// catalog-removal part of DROP/DETACH DATABASE under the real database-level DDL guard.
    ASSERT_TRUE(waitForFailPointPause(FailPoints::udt_lifecycle_pause_after_database_lookup, std::chrono::seconds(10)));
    std::exception_ptr detach_error;
    DatabasePtr detached_database;
    try
    {
        auto drop_database_ddl_guard = DatabaseCatalog::instance().getDDLGuard(String(database_name), "", database.get());
        detached_database = DatabaseCatalog::instance().detachDatabase(context, String(database_name), false, false);
        database_attached_to_catalog = false;
    }
    catch (...)
    {
        detach_error = std::current_exception();
    }

    controlled_disk->armExistsProbe();
    bool exists_probe_armed = true;
    SCOPE_EXIT({
        if (exists_probe_armed)
            static_cast<void>(controlled_disk->disarmExistsProbe());
    });
    FailPointInjection::disableFailPoint(FailPoints::udt_lifecycle_pause_after_database_lookup);
    mutation_thread.join();
    const UInt64 unexpected_storage_probes = controlled_disk->disarmExistsProbe();
    exists_probe_armed = false;

    EXPECT_EQ(detach_error, nullptr);
    EXPECT_EQ(detached_database.get(), database.get());
    EXPECT_EQ(unexpected_storage_probes, 0);
    ASSERT_NE(mutation_error, nullptr);
    try
    {
        std::rethrow_exception(mutation_error);
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::UNFINISHED) << error.message();
    }
    catch (...)
    {
        FAIL() << "Expected DB::Exception from the stale DatabasePtr guard check";
    }
    EXPECT_FALSE(disk->existsFileOrDirectory(String(metadata_root) + "/types"));
}

TEST_F(AtomicLifecycleAdapterTest, ShutdownSerializesAgainstAnInFlightFirstMutation)
{
    controlled_disk->blockNextDirectorySyncContaining("/staging");
    std::exception_ptr mutation_error;
    std::thread mutation_thread(
        [&]
        {
            try
            {
                createOrAttach("CREATE TYPE udt_lifecycle.Concurrent AS UInt64");
            }
            catch (...)
            {
                mutation_error = std::current_exception();
            }
        });

    FailPointInjection::enableFailPoint(FailPoints::udt_authority_shutdown_pause_before_fence);
    bool shutdown_failpoint_enabled = true;
    std::thread shutdown_thread;
    SCOPE_EXIT({
        controlled_disk->releaseBlockedDirectorySync();
        if (shutdown_failpoint_enabled)
            FailPointInjection::disableFailPoint(FailPoints::udt_authority_shutdown_pause_before_fence);
        if (mutation_thread.joinable())
            mutation_thread.join();
        if (shutdown_thread.joinable())
            shutdown_thread.join();
    });

    ASSERT_TRUE(controlled_disk->waitForBlockedDirectorySync(std::chrono::seconds(10)));

    std::atomic<bool> shutdown_finished = false;
    std::exception_ptr shutdown_error;
    shutdown_thread = std::thread(
        [&]
        {
            try
            {
                database->shutdown();
            }
            catch (...)
            {
                shutdown_error = std::current_exception();
            }
            shutdown_finished.store(true, std::memory_order_release);
        });
    ASSERT_TRUE(waitForFailPointPause(FailPoints::udt_authority_shutdown_pause_before_fence, std::chrono::seconds(10)));
    EXPECT_FALSE(shutdown_finished.load(std::memory_order_acquire));

    controlled_disk->releaseBlockedDirectorySync();
    mutation_thread.join();
    EXPECT_EQ(mutation_error, nullptr);
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    const auto * committed_storage = database->getMutationStorageForTest();
    ASSERT_NE(committed_storage, nullptr);
    EXPECT_TRUE(disk->existsFile(committed_storage->getPaths().commitPath(1)));
    FailPointInjection::notifyFailPoint(FailPoints::udt_authority_shutdown_pause_before_fence);
    FailPointInjection::disableFailPoint(FailPoints::udt_authority_shutdown_pause_before_fence);
    shutdown_failpoint_enabled = false;
    shutdown_thread.join();
    EXPECT_EQ(shutdown_error, nullptr);
    EXPECT_TRUE(shutdown_finished.load(std::memory_order_acquire));
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_THROW(database->getUDTLifecycleAdapter().acquireSnapshot(), Exception);
    try
    {
        createOrAttach("CREATE TYPE udt_lifecycle.TooLate AS UInt64");
        FAIL() << "CREATE TYPE unexpectedly started after database shutdown";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::ABORTED);
    }
}

}
}
