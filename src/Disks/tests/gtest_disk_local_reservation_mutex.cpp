#include <Disks/DiskLocal.h>
#include <Disks/tests/gtest_disk.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <latch>
#include <thread>

using namespace DB;

namespace
{

/// A disk whose free-space probe is slow. The probe runs inside the reservation lock,
/// so with a process-wide lock one slow disk stalls reservations on all other disks.
class SlowSpaceDisk final : public DiskLocal
{
public:
    SlowSpaceDisk(const String & name_, const String & path_)
        : DiskLocal(name_, path_)
    {
    }

    std::optional<UInt64> getAvailableSpace() const override
    {
        if (slow.exchange(false))
        {
            entered.count_down();
            std::this_thread::sleep_for(std::chrono::milliseconds(slow_probe_ms));
        }
        return DiskLocal::getAvailableSpace();
    }

    void waitUntilProbeStarted() const { entered.wait(); }

    mutable std::atomic<bool> slow{false};
    static constexpr int slow_probe_ms = 1000;

private:
    mutable std::latch entered{1};
};

}

TEST(DiskLocalReservation, SlowDiskDoesNotStallReservationsOnOtherDisks)
{
    DiskPtr slow_disk_dir = createDisk("slow_probe");
    DiskPtr other_disk = createDisk("other");

    ReservationPtr reservation_on_slow_disk;
    ReservationPtr reservation_on_other_disk;
    Int64 elapsed_ms = 0;
    Int64 slow_reserve_ms = 0;

    {
        auto slow_disk = std::make_shared<SlowSpaceDisk>("slow_probe", slow_disk_dir->getPath());

        slow_disk->slow = true;
        std::thread thread(
            [&]
            {
                const auto started = std::chrono::steady_clock::now();
                reservation_on_slow_disk = slow_disk->reserve(1);
                slow_reserve_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
            });
        slow_disk->waitUntilProbeStarted();

        const auto before = std::chrono::steady_clock::now();
        reservation_on_other_disk = other_disk->reserve(1);
        elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - before).count();

        thread.join();
    }

    destroyDisk(slow_disk_dir);
    destroyDisk(other_disk);

    ASSERT_NE(reservation_on_other_disk, nullptr);
    /// Sanity: the slow probe did run, so a green verdict means "no cross-disk stall",
    /// not "the probe never fired" (which would otherwise surface as a confusing
    /// "refuted" verdict from the bugfix validation).
    ASSERT_GE(slow_reserve_ms, SlowSpaceDisk::slow_probe_ms / 2);
    /// With a process-wide reservation lock the other disk waits out the whole slow
    /// probe of the first one (as the FullDisk note in gtest_volume_jbod_round_robin.cpp
    /// describes); with a per-disk lock it does not.
    ASSERT_LT(elapsed_ms, SlowSpaceDisk::slow_probe_ms / 2) << "reserve() on the other disk took " << elapsed_ms << " ms";
}
