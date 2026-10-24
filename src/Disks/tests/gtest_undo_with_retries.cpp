#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/UndoWithRetries.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <gtest/gtest.h>

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableUndoStageRetries;
}

namespace DB::ErrorCodes
{
    extern const int FAULT_INJECTED;
    extern const int CORRUPTED_DATA;
}

using namespace DB;

/// The reversal of a failed transaction may not give up while object storage rejects it, so the stage is repeated
/// until it goes through. A failing stage needs no object storage to observe that, only a stage that says it failed.
TEST(UndoWithRetries, RepeatsAStageUntilItSucceeds)
{
    const auto retries_before = ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries];

    size_t attempts = 0;
    undoWithRetries(getLogger("UndoWithRetries"), "put the marker back", [&]
    {
        if (++attempts < 3)
            throw Exception(ErrorCodes::FAULT_INJECTED, "Object storage is unavailable");
    });

    EXPECT_EQ(attempts, 3u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries] - retries_before, 2u);
}

/// A stage that goes through the first time is not repeated, and nothing is counted as a retry.
TEST(UndoWithRetries, DoesNotRepeatAStageThatSucceeds)
{
    const auto retries_before = ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries];

    size_t attempts = 0;
    undoWithRetries(getLogger("UndoWithRetries"), "put the marker back", [&] { ++attempts; });

    EXPECT_EQ(attempts, 1u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries], retries_before);
}

/// A stage that finds its blob gone reports `CORRUPTED_DATA`, and asking again cannot bring the blob back. The stage
/// goes through from the fourth attempt, so a repeat shows as a missing exception rather than a hang.
TEST(UndoWithRetries, DoesNotRepeatCorruptedData)
{
    const auto retries_before = ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries];

    size_t attempts = 0;
    EXPECT_THROW(undoWithRetries(getLogger("UndoWithRetries"), "put the marker back", [&]
    {
        if (++attempts < 4)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "The marker is gone");
    }), Exception);

    EXPECT_EQ(attempts, 1u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::DiskPlainRewritableUndoStageRetries], retries_before);
}

/// `LOGICAL_ERROR` is not repeated either. It is not covered here: `DB::Exception` aborts the process for that code in
/// a debug or sanitizer build, which is every build these tests run in, so the case cannot be reached from a test.
