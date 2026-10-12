#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/UndoWithRetries.h>

#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <future>
#include <Common/ProfileEvents.h>

#include <gtest/gtest.h>

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableUndoStageRetries;
}

namespace DB::ErrorCodes
{
    extern const int FAULT_INJECTED;
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

/// `LOGICAL_ERROR` is the one code that is not repeated, because asking again cannot make an invariant hold. It is not
/// covered here: `DB::Exception` aborts the process for that code in a debug or sanitizer build, which is every build
/// these tests run in, so the case cannot be reached from a test.

TEST(UndoWithRetries, CleanupStopsAfterEscalation)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto state = std::make_shared<OperationCancellationState>();
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        state->request(CancellationRequest::StopWork);
        ExecutionPhaseScope cleanup(ExecutionPhase::Cleanup);
        size_t attempts = 0;
        EXPECT_THROW(
            undoWithRetries(getLogger("UndoWithRetries"), "restore metadata", [&]
            {
                ++attempts;
                EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
                state->request(CancellationRequest::StopAll);
                throw Exception(ErrorCodes::FAULT_INJECTED, "Object storage is unavailable");
            }),
            Exception);
        EXPECT_EQ(attempts, 1);
        EXPECT_EQ(CurrentThread::getCancellationRequest(), CancellationRequest::StopAll);
    }).get();
}
