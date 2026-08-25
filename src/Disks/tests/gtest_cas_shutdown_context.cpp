#include <gtest/gtest.h>
#include <Common/ProfileEvents.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Interpreters/Context.h>

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <stdexcept>

using namespace DB::Cas;

namespace DB::ContentAddressedSetting
{
extern const ContentAddressedSettingsBool gc_enabled;
}

namespace ProfileEvents
{
extern const Event CASEventDroppedContextExpired;
}

namespace
{

/// Reuse the single `ContextSharedPart` owned by the global gtest environment, exactly as the
/// interpreter tests do. Each returned `Context` is independently owned, so a test can release or
/// reset its copy without disturbing the process-global context or the other tests.
DB::ContextMutablePtr makeTestContext()
{
    return DB::Context::createCopy(getContext().context);
}

std::shared_ptr<DB::ContentAddressedMetadataStorage> openTestStorage(
    const DB::ContextPtr & context = {}, bool startup = true)
{
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "cas_shutdown_context_scratch");
    /// These tests exercise the event sink synchronously. Keeping the GC scheduler off avoids adding
    /// unrelated worker activity while preserving the real pool event sink installed at `startup`.
    settings[DB::ContentAddressedSetting::gc_enabled] = false;
    settings.validate();

    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", context, settings);
    if (startup)
        storage->startup();
    return storage;
}

void emitTestEvent(DB::ContentAddressedMetadataStorage & storage)
{
    auto pool = storage.poolForTest();
    if (!pool)
        throw std::runtime_error("test storage has no pool");
    EventEmitter{*pool}.emit([](CasEvent & event)
    {
        event.type = CasEventType::Exception;
        event.reason = "test event";
    });
}

/// Open a pool, arm one teardown phase to throw, destroy it, and report whether the clean-release
/// marker was written. Runs inside the subprocess of each exit test below.
[[noreturn]] void tearDownWithThrowingPhase(int phase)
{
    auto backend = std::make_shared<InMemoryBackend>();
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    auto thrower = [] { throw std::runtime_error("injected teardown phase failure"); };
    if (phase == 1)
        config.teardown_phase1_throw_for_test = thrower;
    else if (phase == 2)
        config.teardown_phase2_throw_for_test = thrower;
    else
        config.teardown_phase3_throw_for_test = thrower;

    {
        auto store = Pool::open(backend, config);
        (void)store;
    }   /// `~Pool` runs here.

    /// A failed ref-lane drain must not leave a clean-release marker behind. That marker lets a
    /// successor skip the observation window, so a phase-2 failure must leave it absent.
    const auto mount = backend->get(Layout(config.pool_prefix).mountKey(config.server_root_id));
    const bool clean_release = mount
        && decodeMountLease(mount->bytes).min_active == std::numeric_limits<uint64_t>::max();
    const bool marker_must_be_absent = phase == 2;
    std::_Exit(marker_must_be_absent && clean_release ? 1 : 0);
}

}

TEST(CASShutdownExitTest, TeardownPhase1ThrowExitsCleanly)
{
    EXPECT_EXIT(tearDownWithThrowingPhase(1), ::testing::ExitedWithCode(0), "");
}

TEST(CASShutdownExitTest, TeardownPhase2ThrowExitsCleanlyAndSkipsTheMarker)
{
    EXPECT_EXIT(tearDownWithThrowingPhase(2), ::testing::ExitedWithCode(0), "");
}

TEST(CASShutdownExitTest, TeardownPhase3ThrowExitsCleanly)
{
    EXPECT_EXIT(tearDownWithThrowingPhase(3), ::testing::ExitedWithCode(0), "");
}

/// `Server.cpp` calls `resetSharedContext` immediately before releasing the context. An event emitted
/// in that window must be skipped safely, not dereference a null `shared`.
TEST(CASShutdownExitTest, EmitAfterResetSharedContextExitsCleanly)
{
    EXPECT_EXIT(
        {
            auto context = makeTestContext();
            auto storage = openTestStorage(context);
            ASSERT_TRUE(storage->poolForTest()->hasEventSink());
            emitTestEvent(*storage);
            context->resetSharedContext();
            emitTestEvent(*storage);
            std::_Exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

/// An EXPIRED weak reference is the one case that is counted.
TEST(CASShutdownContext, ExpiredContextDropsTheEventAndCountsIt)
{
    auto context = makeTestContext();
    auto storage = openTestStorage(context);
    ASSERT_TRUE(storage->poolForTest()->hasEventSink());
    const auto before = ProfileEvents::global_counters[ProfileEvents::CASEventDroppedContextExpired].load();

    context.reset();
    emitTestEvent(*storage);

    const auto after = ProfileEvents::global_counters[ProfileEvents::CASEventDroppedContextExpired].load();
    EXPECT_EQ(after - before, 1u);
}

/// `nullptr` at construction means the integration is off. Nothing is emitted and NOTHING is counted --
/// several existing suites construct the storage this way.
TEST(CASShutdownContext, DisabledIntegrationCountsNothing)
{
    auto storage = openTestStorage();
    ASSERT_FALSE(storage->poolForTest()->hasEventSink());
    const auto before = ProfileEvents::global_counters[ProfileEvents::CASEventDroppedContextExpired].load();

    emitTestEvent(*storage);

    const auto after = ProfileEvents::global_counters[ProfileEvents::CASEventDroppedContextExpired].load();
    EXPECT_EQ(after - before, 0u);
}

/// A live context whose system log is not configured is ordinary steady state: no emit, no count.
TEST(CASShutdownContext, MissingSystemLogCountsNothing)
{
    auto context = makeTestContext();
    auto storage = openTestStorage(context);
    ASSERT_TRUE(storage->poolForTest()->hasEventSink());
    const auto before = ProfileEvents::global_counters[ProfileEvents::CASEventDroppedContextExpired].load();

    emitTestEvent(*storage);

    const auto after = ProfileEvents::global_counters[ProfileEvents::CASEventDroppedContextExpired].load();
    EXPECT_EQ(after - before, 0u);
}

/// The storage must no longer keep the context alive. This is the property `Server.cpp` relies on when
/// it destroys the context explicitly.
TEST(CASShutdownContext, StorageDoesNotExtendContextLifetime)
{
    auto context = makeTestContext();
    std::weak_ptr<const DB::Context> weak_context = context;
    auto storage = openTestStorage(context);

    context.reset();

    EXPECT_EQ(weak_context.use_count(), 0L);
}

/// An expired reference supplied at `startup` is an error, not the disabled path.
TEST(CASShutdownContext, ExpiredContextAtStartupFails)
{
    auto context = makeTestContext();
    auto storage = openTestStorage(context, /*startup=*/false);
    context.reset();

    EXPECT_ANY_THROW(storage->startup());
}
