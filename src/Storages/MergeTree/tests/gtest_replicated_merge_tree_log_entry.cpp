#include <gtest/gtest.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>

using namespace DB;

namespace
{

class ServerTimezoneGuard
{
public:
    explicit ServerTimezoneGuard(std::string_view time_zone)
        : previous(DateLUT::serverTimezoneInstance().getTimeZone())
    {
        DateLUT::setDefaultTimezone(time_zone);
    }

    ~ServerTimezoneGuard() { DateLUT::setDefaultTimezone(previous); }

private:
    std::string previous;
};

/// Writes an entry created at `create_time` and parses it back from a log znode created at `znode_ctime`.
time_t roundTrip(time_t create_time, time_t znode_ctime)
{
    ReplicatedMergeTreeLogEntryData entry;
    entry.type = ReplicatedMergeTreeLogEntryData::GET_PART;
    entry.create_time = create_time;
    entry.source_replica = "r1";
    entry.new_part_name = "all_1_1_0";

    Coordination::Stat stat;
    stat.ctime = znode_ctime * 1000;
    return ReplicatedMergeTreeLogEntry::parse(entry.toString(), stat, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        ->create_time;
}

}

TEST(ReplicatedMergeTreeLogEntry, CreateTimeInRepeatedHour)
{
    ServerTimezoneGuard guard("Europe/Amsterdam");

    /// 2026-10-25 02:30:00 CEST and 2026-10-25 02:30:00 CET.
    for (time_t t : {1792888200, 1792891800})
    {
        EXPECT_EQ(roundTrip(t, t), t);
        EXPECT_EQ(roundTrip(t, t + 2), t + 2);
    }

    /// 01:30:00 CEST and 03:30:00 CET are unambiguous, so the written time is kept.
    EXPECT_EQ(roundTrip(1792884600, 1792884600 + 600), 1792884600);
    EXPECT_EQ(roundTrip(1792895400, 1792895400 + 3600), 1792895400);
}

TEST(ReplicatedMergeTreeLogEntry, CreateTimeInRepeatedHalfHour)
{
    ServerTimezoneGuard guard("Australia/Lord_Howe");

    /// 2026-04-05 01:45:00 +11 and 2026-04-05 01:45:00 +10:30.
    for (time_t t : {1775313900, 1775315700})
        EXPECT_EQ(roundTrip(t, t), t);
}
