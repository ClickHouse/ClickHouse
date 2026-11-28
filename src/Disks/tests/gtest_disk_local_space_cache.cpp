#include <Disks/DiskLocal.h>
#include <Disks/DiskSelector.h>
#include <Disks/registerDisks.h>
#include <Disks/tests/gtest_disk.h>
#include <Common/filesystemHelpers.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <semaphore>
#include <sstream>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

namespace
{

constexpr UInt64 short_ttl_ms = 100;
constexpr UInt64 hour_in_milliseconds = 60 * 60 * 1000;
constexpr const char * disks_prefix = "storage_configuration.disks";

/// A directory made by `createDisk`, removed with the object.
struct DiskDirectory
{
    explicit DiskDirectory(const char * name)
        : disk(createDisk(name))
    {
    }
    ~DiskDirectory() { destroyDisk(disk); }
    DiskDirectory(const DiskDirectory &) = delete;
    DiskDirectory & operator=(const DiskDirectory &) = delete;

    DiskPtr disk;
};

/// The definition of a `local` disk for its constructor and `applyNewSettings`: the TTL is written unless it is `nullopt`,
/// the free space to keep unless it is zero.
Poco::AutoPtr<Poco::Util::MapConfiguration>
diskDefinition(const String & path, std::optional<UInt64> space_cache_ttl_ms, UInt64 keep_free_space_bytes = 0)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;
    config->setString("disk.path", path);
    if (space_cache_ttl_ms)
        config->setString("disk.disk_space_cache_ttl_ms", std::to_string(*space_cache_ttl_ms));
    if (keep_free_space_bytes)
        config->setString("disk.keep_free_space_bytes", std::to_string(keep_free_space_bytes));
    return config;
}

/// A disk that counts its `statvfs` calls; a test can make the next call fail or pause it.
class CountingSpaceDisk final : public DiskLocal
{
public:
    CountingSpaceDisk(
        const String & name_, const String & path_, std::optional<UInt64> space_cache_ttl_ms, UInt64 keep_free_space_bytes = 0)
        : DiskLocal(
              name_,
              path_,
              keep_free_space_bytes,
              getContext().context,
              *diskDefinition(path_, space_cache_ttl_ms, keep_free_space_bytes),
              "disk")
    {
    }

    std::optional<UInt64> getTotalSpace() const override
    {
        entered.release();
        return DiskLocal::getTotalSpace();
    }

    DiskSpace readDiskSpace() const override
    {
        ++probes;
        last_probe_start = static_cast<Int64>(std::chrono::steady_clock::now().time_since_epoch().count());
        if (fail_next_probe.exchange(false))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The injected failure of statvfs");
        if (pause_next_probe.exchange(false))
        {
            probe_started.release();
            /// Bounded: a probe that nobody releases must not hang the test.
            if (!probe_release.try_acquire_for(std::chrono::seconds(10)))
                probe_timed_out = true;
        }
        return DiskLocal::readDiskSpace();
    }

    /// The moment the last probe started.
    [[nodiscard]] std::chrono::steady_clock::time_point lastProbeStart() const
    {
        return std::chrono::steady_clock::time_point(std::chrono::steady_clock::duration(last_probe_start.load()));
    }

    void failNextProbe() { fail_next_probe = true; }
    void pauseNextProbe() { pause_next_probe = true; }
    void markReadOnly(bool value) { readonly = value; }
    void markBroken(bool value) { broken = value; }

    /// The check of the access to the disk as the server runs it; the file it writes is removed.
    void runAccessCheck() { checkAccessImpl("clickhouse_access_check_of_test"); }

    /// Waits for a caller to get inside the paused probe; false if none did in time.
    [[nodiscard]] bool waitUntilProbeStarted() const { return probe_started.try_acquire_for(std::chrono::seconds(10)); }

    /// Waits until `count` callers have asked for the total space; false if they did not in time.
    [[nodiscard]] bool waitUntilEntered(size_t count) const
    {
        for (size_t i = 0; i < count; ++i)
            if (!entered.try_acquire_for(std::chrono::seconds(10)))
                return false;
        return true;
    }

    void releaseProbe() { probe_release.release(); }

    /// True if a paused probe ended because nobody released it.
    [[nodiscard]] bool probeTimedOut() const { return probe_timed_out; }

    /// Replaces the definition of the disk with the given TTL of the space cache and free space to keep.
    void reloadDefinition(std::optional<UInt64> space_cache_ttl_ms, UInt64 keep_free_space_bytes = 0)
    {
        applyNewSettings(*diskDefinition(getPath(), space_cache_ttl_ms, keep_free_space_bytes), getContext().context, "disk", {});
    }

    mutable std::atomic<Int64> probes{0};

private:
    mutable std::atomic<Int64> last_probe_start{0};
    mutable std::atomic<bool> probe_timed_out{false};
    mutable std::atomic<bool> fail_next_probe{false};
    mutable std::atomic<bool> pause_next_probe{false};
    mutable std::counting_semaphore<> entered{0};
    mutable std::binary_semaphore probe_started{0};
    mutable std::binary_semaphore probe_release{0};
};

String elementKey(const String & disk_name, const String & element) { return String(disks_prefix) + "." + disk_name + "." + element; }

/// The configuration with one `local` disk, with or without the element of the space cache.
Poco::AutoPtr<Poco::Util::MapConfiguration> configurationWithLocalDisk(const String & path, bool with_element)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;
    config->setString(elementKey("data", "path"), path);
    if (with_element)
        config->setString(elementKey("data", "disk_space_cache_ttl_ms"), "5000");
    return config;
}

/// The configuration with one `object_storage` disk over a local directory, with the element.
Poco::AutoPtr<Poco::Util::MapConfiguration> configurationWithObjectStorageDisk(const String & path, const String & metadata_path)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;
    config->setString(elementKey("objects", "type"), "object_storage");
    config->setString(elementKey("objects", "object_storage_type"), "local");
    config->setString(elementKey("objects", "path"), path);
    config->setString(elementKey("objects", "metadata_type"), "local");
    config->setString(elementKey("objects", "metadata_path"), metadata_path);
    config->setString(elementKey("objects", "disk_space_cache_ttl_ms"), "5000");
    return config;
}

/// The configuration with the given disks, each one a section. It has to be XML: there a section answers to a lookup
/// of its own name with the text of its children, so a disk named like a setting is taken for the setting. A
/// `MapConfiguration` looks every key up exactly and cannot show that.
Poco::AutoPtr<Poco::Util::XMLConfiguration> configurationWithDisks(const String & disks)
{
    const String xml = "<clickhouse><storage_configuration><disks>" + disks + "</disks></storage_configuration></clickhouse>";
    std::istringstream stream(xml); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    return new Poco::Util::XMLConfiguration(stream);
}

}

/// A directory under the temporary directory of the system, with a counting disk on it.
class DiskLocalSpaceCache : public ::testing::Test
{
protected:
    /// A `nullopt` TTL leaves the setting out of the definition of the disk.
    void makeDisk(const char * name, std::optional<UInt64> space_cache_ttl_ms, UInt64 keep_free_space_bytes = 0)
    {
        disk.reset();
        dir = std::make_unique<DiskDirectory>(name);
        disk = std::make_shared<CountingSpaceDisk>(name, dir->disk->getPath(), space_cache_ttl_ms, keep_free_space_bytes);
    }

    /// Declared before `disk`, so that it is destroyed after it.
    std::unique_ptr<DiskDirectory> dir;
    std::shared_ptr<CountingSpaceDisk> disk;
};

TEST_F(DiskLocalSpaceCache, WithoutSettingEveryCallProbesTheDisk)
{
    makeDisk("space_cache_disabled", std::nullopt);

    disk->getTotalSpace();
    disk->getAvailableSpace();
    disk->getUnreservedSpace();
    EXPECT_EQ(disk->probes.load(), 3);

    /// The reservations probe the disk too, without and with constraints.
    Int64 probes = disk->probes.load();
    const auto reservation = disk->reserve(1);
    EXPECT_NE(reservation, nullptr);
    EXPECT_GT(disk->probes.load(), probes);

    probes = disk->probes.load();
    const auto constrained_reservation = disk->reserve(1, ReservationConstraints(0, 0));
    EXPECT_NE(constrained_reservation, nullptr);
    EXPECT_GT(disk->probes.load(), probes);
}

TEST_F(DiskLocalSpaceCache, WithoutTheCacheCallersDoNotWaitForEachOther)
{
    makeDisk("space_cache_no_wait", std::nullopt);
    disk->pauseNextProbe();

    auto paused_caller = std::async(std::launch::async, [disk = disk] { return disk->getTotalSpace(); });
    const auto probe_started = disk->waitUntilProbeStarted();

    /// This caller probes the disk too instead of waiting for the paused probe to end.
    const auto other_total = disk->getTotalSpace();
    disk->releaseProbe();

    ASSERT_TRUE(probe_started);
    EXPECT_FALSE(disk->probeTimedOut()) << "A caller waited for the probe of another caller";
    ASSERT_TRUE(paused_caller.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    EXPECT_TRUE(paused_caller.get().has_value());
    EXPECT_TRUE(other_total.has_value());
    EXPECT_EQ(disk->probes.load(), 2);
}

TEST_F(DiskLocalSpaceCache, ConcurrentCallersShareOneProbe)
{
    constexpr size_t callers = 8;
    const auto short_ttl = std::chrono::milliseconds(short_ttl_ms);

    /// Callers that come while another one probes wait instead of probing each on their own. When the probe takes longer
    /// than the TTL, its value is expired once stored: the first of the waiters probes anew, and the rest share that probe.
    for (bool probe_outlives_ttl : {false, true})
    {
        /// With the short TTL the attempt says nothing if more than the TTL passes between the probe of one waiter and the
        /// check of a later one: it is repeated then, and fails if that keeps happening.
        for (size_t attempt = 0; attempt < 4; ++attempt)
        {
            makeDisk(
                probe_outlives_ttl ? "space_cache_shared_slow" : "space_cache_shared",
                probe_outlives_ttl ? short_ttl_ms : hour_in_milliseconds);
            disk->pauseNextProbe();

            std::array<std::optional<UInt64>, callers> totals;
            std::array<std::thread, callers> threads;

            /// The first caller skips the double's `getTotalSpace`, so that only the others are waited for.
            threads[0] = std::thread([&] { totals[0] = disk->DiskLocal::getTotalSpace(); });
            const auto probe_started = disk->waitUntilProbeStarted();

            if (probe_outlives_ttl)
                std::this_thread::sleep_for(2 * short_ttl);

            for (size_t i = 1; i < callers; ++i)
                threads[i] = std::thread([&, i] { totals[i] = disk->getTotalSpace(); });

            /// By the time the probe is released every other caller has called `getTotalSpace()`: none of them probes
            /// while the first one is paused, and after the release at most one of them probes for all the rest.
            const auto all_entered = disk->waitUntilEntered(callers - 1);
            disk->releaseProbe();

            for (auto & thread : threads)
                thread.join();

            ASSERT_TRUE(probe_started);
            EXPECT_FALSE(disk->probeTimedOut());
            ASSERT_TRUE(all_entered);

            const Int64 expected_probes = probe_outlives_ttl ? 2 : 1;
            if (disk->probes.load() == expected_probes)
            {
                ASSERT_TRUE(totals[0].has_value());
                ASSERT_TRUE(totals[1].has_value());
                EXPECT_GT(*totals[1], 0);
                for (size_t i = 2; i < callers; ++i)
                    ASSERT_EQ(totals[i], totals[1]);
                break;
            }

            EXPECT_LT(attempt, 3) << disk->probes.load() << " probes instead of " << expected_probes;
        }
    }
}

TEST_F(DiskLocalSpaceCache, ReservationUsesOneProbeAndUnreservedSpaceStaysLive)
{
    makeDisk("space_cache_unreserved", hour_in_milliseconds);

    const auto unreserved = disk->getUnreservedSpace();
    ASSERT_TRUE(unreserved.has_value());

    /// The constraints need the total space too: both come from the same cached probe.
    constexpr UInt64 reserved_bytes = 1024;
    const auto reservation = disk->reserve(reserved_bytes, ReservationConstraints(0, 0));
    ASSERT_NE(reservation, nullptr);
    EXPECT_EQ(disk->probes.load(), 1);

    /// The reservation is subtracted from the cached available space without a new probe.
    const auto unreserved_after = disk->getUnreservedSpace();
    ASSERT_TRUE(unreserved_after.has_value());
    EXPECT_EQ(*unreserved_after, *unreserved - reserved_bytes);
    EXPECT_EQ(disk->probes.load(), 1);
}

TEST_F(DiskLocalSpaceCache, ReportsTheFilesystemSpaceMinusTheKeptFreeSpace)
{
    /// Small, so that the test does not need free space on the file system of the temporary directory.
    constexpr UInt64 keep_free_space = 1024;
    makeDisk("space_cache_keep_free", hour_in_milliseconds, keep_free_space);

    const struct statvfs filesystem_stat = getStatVFS(disk->getPath());
    const UInt64 total_on_the_filesystem = filesystem_stat.f_blocks * filesystem_stat.f_frsize;
    const UInt64 free_on_the_filesystem = filesystem_stat.f_bavail * filesystem_stat.f_frsize;

    const auto total = disk->getTotalSpace();
    const auto available = disk->getAvailableSpace();
    ASSERT_TRUE(total.has_value());
    ASSERT_TRUE(available.has_value());
    EXPECT_EQ(*total, total_on_the_filesystem - keep_free_space);

    /// The free space can change while the test runs, unlike the size.
    const UInt64 difference
        = *available > free_on_the_filesystem ? *available - free_on_the_filesystem : free_on_the_filesystem - *available;
    EXPECT_LE(difference, keep_free_space + 64ULL * 1024 * 1024);

    /// Only the raw space is cached, so the new limit applies without a new probe.
    disk->reloadDefinition(hour_in_milliseconds, 2 * keep_free_space);
    const auto total_after = disk->getTotalSpace();
    const auto available_after = disk->getAvailableSpace();
    ASSERT_TRUE(total_after.has_value());
    ASSERT_TRUE(available_after.has_value());
    EXPECT_EQ(*total_after, total_on_the_filesystem - 2 * keep_free_space);
    EXPECT_EQ(*available_after, *available - keep_free_space);
    EXPECT_EQ(disk->probes.load(), 1);

    /// Keeping more free space than the disk has leaves nothing to report, again without a new probe.
    disk->reloadDefinition(hour_in_milliseconds, total_on_the_filesystem + 1);
    EXPECT_EQ(disk->getTotalSpace(), std::optional<UInt64>(0));
    EXPECT_EQ(disk->getAvailableSpace(), std::optional<UInt64>(0));
    EXPECT_EQ(disk->probes.load(), 1);
}

TEST_F(DiskLocalSpaceCache, CachedValueLivesForTtlFromTheStartOfItsProbe)
{
    /// Checked here: the age of a value counts from the start of its probe, each value gets a TTL of its own, a call
    /// served from the cache does not prolong the value, and an expired value is not served.
    /// The TTL is long enough for a call made right after a probe to fall into it even on a loaded machine.
    const auto ttl = std::chrono::milliseconds(short_ttl_ms);
    makeDisk("space_cache_window", short_ttl_ms);
    disk->pauseNextProbe();

    auto paused_caller = std::async(std::launch::async, [disk = disk] { return disk->getTotalSpace(); });

    /// The probe takes longer than the TTL, so its value is already expired when it is stored: the next call probes.
    const auto probe_started = disk->waitUntilProbeStarted();
    std::this_thread::sleep_for(2 * ttl);
    disk->releaseProbe();

    ASSERT_TRUE(probe_started);
    EXPECT_FALSE(disk->probeTimedOut());
    ASSERT_TRUE(paused_caller.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    ASSERT_TRUE(paused_caller.get().has_value());
    disk->getTotalSpace();
    EXPECT_EQ(disk->probes.load(), 2);

    /// A call in the middle of the TTL is served from the cache and does not move the end of the TTL: a call after the
    /// TTL probes again. Calls delayed by more than that say nothing, so the pair is repeated until both landed where
    /// they were meant to.
    bool checked = false;
    for (size_t attempt = 0; attempt < 10 && !checked; ++attempt)
    {
        const auto probed_at = disk->lastProbeStart();
        const auto probes_before = disk->probes.load();

        std::this_thread::sleep_for(ttl * 3 / 5);
        disk->getTotalSpace();
        const auto returned_inside_at = std::chrono::steady_clock::now();
        std::this_thread::sleep_for(ttl * 3 / 5);
        const auto called_after_at = std::chrono::steady_clock::now();
        disk->getTotalSpace();

        if (returned_inside_at - probed_at < ttl && called_after_at - probed_at >= ttl)
        {
            EXPECT_EQ(disk->probes.load(), probes_before + 1);
            checked = true;
        }
    }
    EXPECT_TRUE(checked) << "No pair of calls landed inside the TTL and after it";
}

TEST_F(DiskLocalSpaceCache, FailedProbePropagatesAndIsRetried)
{
    makeDisk("space_cache_failed_probe", hour_in_milliseconds);

    const auto before = disk->getAvailableSpace();
    ASSERT_TRUE(before.has_value());

    /// The disk was unusable, so the cached value is dropped, and the probe that has to replace it fails.
    disk->markBroken(true);
    disk->runAccessCheck();
    disk->failNextProbe();
    EXPECT_THROW(disk->getAvailableSpace(), DB::Exception);

    /// Nothing is left to serve: the next call probes the disk again instead of returning the value cached before.
    const auto after = disk->getAvailableSpace();
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(disk->probes.load(), 3);
}

TEST_F(DiskLocalSpaceCache, UnusableDiskReportsZeroAndDropsCache)
{
    for (bool is_broken : {false, true})
    {
        makeDisk(is_broken ? "space_cache_broken" : "space_cache_readonly", hour_in_milliseconds);

        const auto available = disk->getAvailableSpace();
        ASSERT_TRUE(available.has_value());
        EXPECT_GT(*available, 0);

        /// Another caller holds the cache with a probe while the disk becomes unusable: the space getters answer at once
        /// instead of waiting for that probe.
        disk->reloadDefinition(2 * hour_in_milliseconds);
        disk->pauseNextProbe();
        auto paused_caller = std::async(std::launch::async, [disk = disk] { return disk->getTotalSpace(); });
        const auto probe_started = disk->waitUntilProbeStarted();

        if (is_broken)
            disk->markBroken(true);
        else
            disk->markReadOnly(true);

        EXPECT_EQ(disk->getTotalSpace(), std::optional<UInt64>(0));
        EXPECT_EQ(disk->getAvailableSpace(), std::optional<UInt64>(0));
        EXPECT_EQ(disk->getUnreservedSpace(), std::optional<UInt64>(0));

        disk->releaseProbe();
        ASSERT_TRUE(probe_started);
        ASSERT_TRUE(paused_caller.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        EXPECT_FALSE(disk->probeTimedOut()) << "A caller waited for the probe of another caller on an unusable disk";
        EXPECT_TRUE(paused_caller.get().has_value());
        EXPECT_EQ(disk->probes.load(), 2);

        /// The check finds the disk usable again: the value cached before is not reused.
        disk->runAccessCheck();
        disk->getTotalSpace();
        EXPECT_EQ(disk->probes.load(), 3);

        /// A check of a usable disk changes nothing.
        disk->runAccessCheck();
        disk->getTotalSpace();
        EXPECT_EQ(disk->probes.load(), 3);
    }
}

TEST_F(DiskLocalSpaceCache, ChangedTtlDropsCache)
{
    makeDisk("space_cache_ttl_changes", hour_in_milliseconds);
    disk->getAvailableSpace();

    /// With the cache off every call probes the disk.
    disk->reloadDefinition(std::nullopt);
    disk->getAvailableSpace();
    EXPECT_EQ(disk->probes.load(), 2);

    /// Once it is on again, the value cached before it was turned off is not served.
    disk->reloadDefinition(hour_in_milliseconds);
    disk->getAvailableSpace();
    disk->getAvailableSpace();
    EXPECT_EQ(disk->probes.load(), 3);

    /// Another TTL drops the value too; reloading the same one keeps it.
    disk->reloadDefinition(2 * hour_in_milliseconds);
    disk->getAvailableSpace();
    EXPECT_EQ(disk->probes.load(), 4);
    disk->reloadDefinition(2 * hour_in_milliseconds);
    disk->getAvailableSpace();
    EXPECT_EQ(disk->probes.load(), 4);

    /// A shorter TTL is in force: the next value expires sooner.
    disk->reloadDefinition(short_ttl_ms);
    disk->getAvailableSpace();
    EXPECT_EQ(disk->probes.load(), 5);
    std::this_thread::sleep_for(3 * std::chrono::milliseconds(short_ttl_ms));
    disk->getAvailableSpace();
    EXPECT_EQ(disk->probes.load(), 6);
}

/// The definitions of disks are checked by `DiskSelector`, when the disks are created and when the configuration is reloaded.
class DiskLocalSpaceCacheElement : public ::testing::Test
{
protected:
    static void SetUpTestSuite() { DB::registerDisks(/* global_skip_access_check */ true); }
    static void TearDownTestSuite() { DB::clearDiskRegistry(); }

    /// True if `action` was refused with `UNKNOWN_ELEMENT_IN_CONFIG` that names `element`.
    template <class F>
    [[nodiscard]] bool isRefused(F && action, const String & element)
    {
        try
        {
            action();
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG) << e.message();
            return e.code() == ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG && e.message().contains(": " + element + ".");
        }
        ADD_FAILURE() << "The definition was accepted, `" << element << "` was not reported";
        return false;
    }

    ContextPtr context = getContext().context;
};

TEST_F(DiskLocalSpaceCacheElement, IsAnElementOfLocalDisks)
{
    DiskDirectory dir("space_cache_element");
    auto with_element = configurationWithLocalDisk(dir.disk->getPath(), true);

    /// A disk created with the element.
    DiskSelector created;
    ASSERT_NO_THROW(created.initialize(*with_element, disks_prefix, context));
    EXPECT_NE(created.tryGet("data"), nullptr);

    /// A disk that gets the element by a reload.
    DiskSelector without_element;
    ASSERT_NO_THROW(without_element.initialize(*configurationWithLocalDisk(dir.disk->getPath(), false), disks_prefix, context));
    const auto reloaded = without_element.updateFromConfig(*with_element, disks_prefix, context);
    ASSERT_NE(reloaded, nullptr);
    EXPECT_NE(reloaded->tryGet("data"), nullptr);

    /// The `default` disk with a section of its own that holds the element: created from the section, and given it by a
    /// reload while the disk is implicit.
    Poco::AutoPtr<Poco::Util::MapConfiguration> section = new Poco::Util::MapConfiguration;
    section->setString(elementKey("default", "disk_space_cache_ttl_ms"), "5000");
    DiskSelector created_with_section;
    ASSERT_NO_THROW(created_with_section.initialize(*section, disks_prefix, context));
    EXPECT_NE(created_with_section.tryGet("default"), nullptr);

    Poco::AutoPtr<Poco::Util::MapConfiguration> no_sections = new Poco::Util::MapConfiguration;
    DiskSelector implicit;
    ASSERT_NO_THROW(implicit.initialize(*no_sections, disks_prefix, context));
    const auto implicit_reloaded = implicit.updateFromConfig(*section, disks_prefix, context);
    ASSERT_NE(implicit_reloaded, nullptr);
    EXPECT_NE(implicit_reloaded->tryGet("default"), nullptr);
}

TEST_F(DiskLocalSpaceCacheElement, IsRefusedForAnotherDiskType)
{
    DiskDirectory dir("space_cache_element_refused");

    /// The metadata disk of an object storage disk is a `DiskLocal` too, but the element is not an option of that disk type.
    auto with_element = configurationWithObjectStorageDisk(dir.disk->getPath(), dir.disk->getPath() + "metadata/");
    DiskSelector disks;
    EXPECT_TRUE(isRefused([&] { disks.initialize(*with_element, disks_prefix, context); }, "disk_space_cache_ttl_ms"));
}

TEST_F(DiskLocalSpaceCacheElement, DiskNamedLikeTheElementDoesNotBreakTheImplicitDefaultDisk)
{
    DiskDirectory dir("space_cache_element_named_disk");

    /// The implicit `default` disk has no section of its own; a disk named like the element must not be taken for one.
    const String named_like_the_element = "<disk_space_cache_ttl_ms><path>" + dir.disk->getPath() + "</path></disk_space_cache_ttl_ms>";
    auto configuration = configurationWithDisks(named_like_the_element);
    DiskSelector disks;
    ASSERT_NO_THROW(disks.initialize(*configuration, disks_prefix, context));
    EXPECT_NE(disks.tryGet("default"), nullptr);
    EXPECT_NE(disks.tryGet("disk_space_cache_ttl_ms"), nullptr);
}
