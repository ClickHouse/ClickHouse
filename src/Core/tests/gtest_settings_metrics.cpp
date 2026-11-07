#include <Core/Settings.h>
#include <Core/SettingsMetrics.h>
#include <Core/SettingsSnapshot.h>
#include <gtest/gtest.h>

#include <array>
#include <stdexcept>
#include <thread>

namespace
{

using namespace DB;

auto metrics()
{
    return std::array{
        CurrentMetrics::get(CurrentMetrics::SettingsObjects),
        CurrentMetrics::get(CurrentMetrics::SettingsImplementations),
        CurrentMetrics::get(CurrentMetrics::SettingsDenseData),
        CurrentMetrics::get(CurrentMetrics::SettingsSnapshotStates),
        CurrentMetrics::get(CurrentMetrics::SettingsSnapshotChunks),
        CurrentMetrics::get(CurrentMetrics::SettingsStructuralMemoryBytes)};
}

struct ThrowingValue
{
    static inline bool fail = false;
    int value;

    explicit ThrowingValue(int value_)
        : value(value_)
    {
    }
    ThrowingValue(const ThrowingValue & other)
        : value(other.value)
    {
        if (fail)
            throw std::runtime_error("injected settings field copy failure");
    }
};

struct ThrowingDescription
{
    struct Data
    {
        ThrowingValue value{7};
    };

    static constexpr std::array fields{settingsSnapshotField<ThrowingValue>(0)};
};

}

GTEST_TEST(SettingsMetrics, CopyMoveAssignmentAndLastOwner)
{
    Settings parent;
    parent.set("max_query_size", UInt64(100000));
    const auto before = metrics();
    {
        Settings child(parent);
        const auto copied = metrics();
        EXPECT_EQ(copied[0], before[0] + 1);
        EXPECT_EQ(copied[1], before[1] + 1);
        EXPECT_EQ(copied[2], before[2]);
        EXPECT_EQ(copied[3], before[3]);
        EXPECT_EQ(copied[4], before[4]);
        EXPECT_GT(copied[5], before[5]);

        Settings moved(std::move(child));
        const auto after_move = metrics();
        EXPECT_EQ(after_move[0], copied[0] + 1);
        EXPECT_EQ(after_move[1], copied[1]);
        EXPECT_EQ(after_move[5], copied[5] + sizeof(Settings));

        moved.set("max_query_size", UInt64(100001));
        const auto detached = metrics();
        EXPECT_EQ(detached[3], copied[3] + 1);
        EXPECT_EQ(detached[4], copied[4] + 1);
        moved = parent;
        EXPECT_EQ(metrics(), after_move);

        auto cross_thread = std::make_unique<Settings>(parent);
        cross_thread->set("max_query_size", UInt64(100002));
        std::thread([owner = std::move(cross_thread)] { }).join();
        EXPECT_EQ(metrics(), after_move);
    }
    EXPECT_EQ(metrics(), before);
}

GTEST_TEST(SettingsMetrics, EmbeddedWrappersAreCountedOnce)
{
    struct CachedSettings
    {
        Settings input;
        Settings output;
        bool sanity_clamp = false;
    };

    Settings warmup;
    const auto before = metrics();
    Int64 unadjusted_bytes = 0;
    {
        auto cache = std::allocate_shared<CachedSettings>(SettingsSnapshotAllocator<CachedSettings>{true});
        unadjusted_bytes = metrics()[5] - before[5];
    }
    EXPECT_EQ(metrics(), before);

    {
        using Allocator = SettingsSnapshotAllocator<CachedSettings, SettingsAllocationKind::SnapshotState, 2 * sizeof(Settings)>;
        auto cache = std::allocate_shared<CachedSettings>(Allocator{true});
        const auto allocated = metrics();
        EXPECT_EQ(allocated[0], before[0] + 2);
        EXPECT_EQ(allocated[1], before[1] + 2);
        EXPECT_EQ(allocated[3], before[3] + 1);
        EXPECT_EQ(allocated[5] - before[5], unadjusted_bytes - 2 * sizeof(Settings));
        std::thread([last_owner = std::move(cache)] { }).join();
    }
    EXPECT_EQ(metrics(), before);
}

GTEST_TEST(SettingsMetrics, FailedChunkConstructionReleasesAccounting)
{
    SettingsSnapshot<ThrowingDescription> value;
    EXPECT_EQ(static_cast<const ThrowingValue *>(std::as_const(value).getSettingPointer(0))->value, 7);
    const auto initial = metrics();
    /// Detach the state, then retain the already detached table when chunk construction fails.
    static_cast<void>(value.getSettingPointer(0));
    auto parent(value);
    ThrowingValue::fail = true;
    EXPECT_THROW(value.getSettingPointer(0), std::runtime_error);
    ThrowingValue::fail = false;
    const auto after_failed_detach = metrics();
    /// The failed attempt owns its new table, but must not retain a replacement chunk or buffer.
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::SettingsSnapshotChunks), initial[4] + 1);
    value = parent;
    const auto restored = metrics();
    EXPECT_EQ(restored[3], after_failed_detach[3] - 1);
    EXPECT_EQ(restored[4], after_failed_detach[4]);
    EXPECT_LT(restored[5], after_failed_detach[5]);
    {
        auto descendant(parent);
        const auto before = metrics();
        ThrowingValue::fail = true;
        EXPECT_THROW(descendant.getSettingPointer(0), std::runtime_error);
        ThrowingValue::fail = false;
        descendant = parent;
        EXPECT_EQ(metrics(), before);
    }
}
