#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
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

TEST(UndoWithRetries, CleanupRetriesDespiteCancellation)
{
    std::async(std::launch::async, []
    {
        ThreadStatus status;
        auto state = std::make_shared<OperationCancellationState>();
        auto group = ThreadGroup::createForMergeMutate(getContext().context, [state] { return state->getRequest(); });
        ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
        bool metadata_changed = false;
        size_t attempts = 0;

        struct ChangeMetadata : IMetadataOperation
        {
            bool & metadata_changed;
            size_t & attempts;

            ChangeMetadata(bool & metadata_changed_, size_t & attempts_)
                : metadata_changed(metadata_changed_), attempts(attempts_)
            {
            }

            void execute() override
            {
                metadata_changed = true;
            }

            void undo() override
            {
                EXPECT_TRUE(metadata_changed);
                undoWithRetries(getLogger("UndoWithRetries"), "restore metadata", [&]
                {
                    /// A missing rollback blocker must fail this test instead of retrying cancellation indefinitely.
                    if (++attempts > 2)
                        throw Exception(ErrorCodes::CORRUPTED_DATA, "Metadata rollback exceeded the test retry limit");
                    CurrentThread::checkIfNotCancelled();
                    if (attempts == 1)
                        throw Exception(ErrorCodes::FAULT_INJECTED, "Object storage is unavailable");
                    metadata_changed = false;
                });
            }
        };

        struct CancelAndFail : IMetadataOperation
        {
            OperationCancellationState & state;

            explicit CancelAndFail(OperationCancellationState & state_) : state(state_)
            {
            }

            void execute() override
            {
                state.request(CancellationRequest::StopWork);
                throw Exception(ErrorCodes::FAULT_INJECTED, "Metadata operation failed");
            }
        };

        MetadataOperationsHolder operations;
        operations.addOperation(std::make_unique<ChangeMetadata>(metadata_changed, attempts));
        operations.addOperation(std::make_unique<CancelAndFail>(*state));
        try
        {
            operations.commit();
            FAIL() << "The metadata operation must fail";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::FAULT_INJECTED);
            EXPECT_NE(e.message().find("Metadata operation failed"), String::npos);
        }
        EXPECT_FALSE(metadata_changed);
        EXPECT_EQ(attempts, 2u);
        EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
        EXPECT_EQ(CurrentThread::get().getCancellationRequest(), CancellationRequest::StopWork);
        EXPECT_THROW(CurrentThread::checkIfNotCancelled(), Exception);
    }).get();
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
