#include <Disks/VolumeJBOD.h>
#include <Disks/tests/gtest_disk.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <latch>
#include <optional>
#include <thread>

using namespace DB;

namespace
{

/// A disk without room. Its first reservation pauses until `resume`, so that a test can let other
/// reservations take turns between the first attempt of a reservation and its next one. It does not reserve
/// through `DiskLocal`, whose global reservation mutex would block the other disks during the pause.
class FullDisk : public DiskLocal
{
public:
    using DiskLocal::DiskLocal;
    using DiskLocal::reserve;

    ReservationPtr reserve(UInt64 /* bytes */) override
    {
        if (first_reservation.exchange(false))
        {
            paused.count_down();
            resumed.wait();
        }
        return {};
    }

    void waitUntilPaused() { paused.wait(); }

    void resume() { resumed.count_down(); }

private:
    std::atomic<bool> first_reservation = true;
    std::latch paused{1};
    std::latch resumed{1};
};

class VolumeJBODRoundRobin : public testing::Test
{
protected:
    /// Creates a round-robin volume of `disks_num` disks, of which disk 0 has no room.
    void createVolume(size_t disks_num)
    {
        /// The full disk uses the directory of a disk made by `createDisk`, which `destroyDisk` removes.
        created_disks.push_back(createDisk("full"));
        full_disk = std::make_shared<FullDisk>("full", created_disks.back()->getPath());
        disks.push_back(full_disk);
        for (size_t i = 1; i < disks_num; ++i)
        {
            created_disks.push_back(createDisk());
            disks.push_back(created_disks.back());
        }
        volume = std::make_shared<VolumeJBOD>(
            "volume",
            disks,
            /* max_data_part_size_= */ 0,
            /* are_merges_avoided_= */ false,
            /* perform_ttl_move_on_insert_= */ false,
            VolumeLoadBalancing::ROUND_ROBIN,
            /* least_used_ttl_ms_= */ 0);
    }

    void TearDown() override
    {
        volume.reset();
        disks.clear();
        full_disk.reset();
        for (auto & disk : created_disks)
            destroyDisk(disk);
    }

    /// The position of the disk of `reservation` in the volume, or nothing without a reservation.
    std::optional<size_t> diskOf(const ReservationPtr & reservation) const
    {
        if (!reservation)
            return {};
        return std::ranges::find(disks, reservation->getDisk()) - disks.begin();
    }

    /// Reservation A takes the first turn, which falls on the full disk, and pauses in its attempt there.
    /// Then `others_num` reservations take the next turns, each on the disk of its turn, and A goes on.
    /// Returns the position of the disk that A takes.
    std::optional<size_t> reserveAroundPausedReservation(size_t others_num)
    {
        ReservationPtr paused_reservation;
        std::thread paused_thread([&] { paused_reservation = volume->reserve(1); });
        full_disk->waitUntilPaused();
        for (size_t turn = 1; turn <= others_num; ++turn)
            EXPECT_EQ(diskOf(volume->reserve(1)), turn % disks.size());
        full_disk->resume();
        paused_thread.join();
        return diskOf(paused_reservation);
    }

    std::shared_ptr<FullDisk> full_disk;
    Disks created_disks;
    Disks disks;
    VolumePtr volume;
};

}

TEST_F(VolumeJBODRoundRobin, NextAttemptTakesNextTurn)
{
    /// A takes turn 0 on the full disk, and B takes turn 1 and disk 1. The next attempt of A takes turn 2 and
    /// disk 2, as a reservation of its own would, so that disk 1 does not take both reservations.
    createVolume(3);
    EXPECT_EQ(reserveAroundPausedReservation(1), 2u);
}

TEST_F(VolumeJBODRoundRobin, TurnOnTriedDiskTakesUntriedDisk)
{
    /// A takes turn 0 on the full disk, and B takes turn 1 and disk 1. Turn 2 of A falls on the full disk
    /// again, which A has already tried, so A takes the disk it has not tried yet.
    createVolume(2);
    EXPECT_EQ(reserveAroundPausedReservation(1), 1u);
}

TEST_F(VolumeJBODRoundRobin, RoundOfTurnPicksUntriedDisk)
{
    /// A takes turn 0 on the full disk, and B and C take turns 1 and 2. Turn 3 of A falls on the full disk
    /// again. It is in round 1, so A takes the second of its untried disks 1 and 2.
    createVolume(3);
    EXPECT_EQ(reserveAroundPausedReservation(2), 2u);
}
