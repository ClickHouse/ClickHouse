#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueIFileMetadata.h>

using namespace DB;

namespace
{

using FileStatus = ObjectStorageQueueIFileMetadata::FileStatus;
using State = FileStatus::State;

}

/// `system.s3queue_metadata_cache` shows the state of a file together with the data of the
/// attempt which led to it, so a state observed in keeper must not be shown next to the data
/// of an attempt of this server which did not produce it.
TEST(ObjectStorageQueueFileStatus, StateCommittedElsewhereDropsForeignMarkerAndLocalAttempt)
{
    FileStatus status("file.csv");

    /// An attempt of this server which failed.
    status.onProcessing();
    status.processed_rows = 10;
    status.onFailed("Cannot parse input");

    EXPECT_EQ(status.state.load(), State::Failed);
    EXPECT_EQ(status.foreign_processing_time.load(), 0);

    /// Keeper says that the file is held by another processor. The data of the failed attempt
    /// is kept: it is all we know about the file, and `foreign_processing_time` tells that the
    /// `Processing` state itself is not ours.
    status.onStateObservedInKeeper(State::Processing);

    EXPECT_EQ(status.state.load(), State::Processing);
    EXPECT_NE(status.foreign_processing_time.load(), 0);
    EXPECT_EQ(status.processed_rows.load(), 10);
    EXPECT_EQ(status.getException(), "Cannot parse input");

    /// That processor committed the file. Neither the marker of the state it replaces nor the
    /// data of the attempt of this server describe the file being `Processed`.
    status.onStateObservedInKeeper(State::Processed);

    EXPECT_EQ(status.state.load(), State::Processed);
    EXPECT_EQ(status.foreign_processing_time.load(), 0);
    EXPECT_EQ(status.processed_rows.load(), 0);
    EXPECT_EQ(status.processing_start_time.load(), 0);
    EXPECT_EQ(status.processing_end_time.load(), 0);
    EXPECT_EQ(status.getException(), "");
}

/// The reverse regression of the test above: the data of an attempt must survive an observation
/// which does not change the state. The failed node which keeper reports can be the one created
/// by this server, and then its exception is exactly what the observed state is about.
TEST(ObjectStorageQueueFileStatus, StateObservedAgainKeepsLocalAttempt)
{
    FileStatus status("file.csv");

    status.onProcessing();
    status.processed_rows = 10;
    status.onFailed("Cannot parse input");

    status.onStateObservedInKeeper(State::Failed);

    EXPECT_EQ(status.state.load(), State::Failed);
    EXPECT_EQ(status.foreign_processing_time.load(), 0);
    EXPECT_EQ(status.processed_rows.load(), 10);
    EXPECT_NE(status.processing_start_time.load(), 0);
    EXPECT_NE(status.processing_end_time.load(), 0);
    EXPECT_EQ(status.getException(), "Cannot parse input");
}

/// A file status is shared by all processors of one keeper path on this server, so a processor
/// can observe in keeper the processing node of a processor of the same server, and mark the
/// state it does not own. Committing the file must not leave that marker behind.
TEST(ObjectStorageQueueFileStatus, CommitDropsForeignMarkerOfALocallyOwnedState)
{
    FileStatus processed_status("file.csv");

    processed_status.onProcessing();
    processed_status.onStateObservedInKeeper(State::Processing);
    processed_status.setProcessingEndTime();
    processed_status.onProcessed();

    EXPECT_EQ(processed_status.state.load(), State::Processed);
    EXPECT_EQ(processed_status.foreign_processing_time.load(), 0);

    FileStatus failed_status("file.csv");

    failed_status.onProcessing();
    failed_status.onStateObservedInKeeper(State::Processing);
    failed_status.onFailed("Cannot parse input");

    EXPECT_EQ(failed_status.state.load(), State::Failed);
    EXPECT_EQ(failed_status.foreign_processing_time.load(), 0);
}
