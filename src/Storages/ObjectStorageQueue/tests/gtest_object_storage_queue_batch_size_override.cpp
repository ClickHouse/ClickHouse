#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueBatchSizeOverride.h>

using namespace DB;

TEST(ObjectStorageQueueBatchSizeOverride, FailuresHalveDownToOne)
{
    ObjectStorageQueueBatchSizeOverride batch_size;
    batch_size.halveAfterFailure(/*default_max_files=*/ 10, /*processed_files=*/ 3);
    EXPECT_EQ(batch_size.get().max_files, 5u);
    batch_size.halveAfterFailure(10, 3);
    batch_size.halveAfterFailure(10, 3);
    batch_size.halveAfterFailure(10, 3);
    EXPECT_EQ(batch_size.get().max_files, 1u);

    /// Without a configured limit, the size of the failed batch is halved.
    ObjectStorageQueueBatchSizeOverride unlimited;
    unlimited.halveAfterFailure(0, 8);
    EXPECT_EQ(unlimited.get().max_files, 4u);
}

TEST(ObjectStorageQueueBatchSizeOverride, SuccessStartedBeforeFailureDoesNotReset)
{
    ObjectStorageQueueBatchSizeOverride batch_size;
    const auto started = batch_size.get();
    batch_size.halveAfterFailure(10, 10);
    batch_size.resetAfterSuccess(started);
    EXPECT_EQ(batch_size.get().max_files, 5u);

    /// A success which started after the last failure restores the full batch size.
    batch_size.resetAfterSuccess(batch_size.get());
    EXPECT_EQ(batch_size.get().max_files, 0u);
}

/// The same value of the limit before and after a newer failure must not let an older success clear it.
TEST(ObjectStorageQueueBatchSizeOverride, OlderSuccessDoesNotClearNewerFailureWithSameValue)
{
    ObjectStorageQueueBatchSizeOverride batch_size;
    batch_size.halveAfterFailure(10, 10);
    ASSERT_EQ(batch_size.get().max_files, 5u);

    const auto slow_success_started = batch_size.get();   /// Task A starts a slow batch at 5.
    batch_size.halveAfterFailure(10, 5);                  /// Task B fails: 5 -> 2.
    const auto fast_success_started = batch_size.get();   /// Task C starts at 2...
    batch_size.resetAfterSuccess(fast_success_started);   /// ... and succeeds: back to no override.
    ASSERT_EQ(batch_size.get().max_files, 0u);
    batch_size.halveAfterFailure(10, 10);                 /// Task D fails: 10 -> 5.
    ASSERT_EQ(batch_size.get().max_files, 5u);

    batch_size.resetAfterSuccess(slow_success_started);   /// Task A finishes.
    EXPECT_EQ(batch_size.get().max_files, 5u);
}

TEST(ObjectStorageQueueBatchSizeOverride, ResetInvalidatesInFlightSnapshots)
{
    ObjectStorageQueueBatchSizeOverride batch_size;
    const auto started = batch_size.get();
    batch_size.reset();
    batch_size.halveAfterFailure(10, 10);
    batch_size.resetAfterSuccess(started);
    EXPECT_EQ(batch_size.get().max_files, 5u);
}
