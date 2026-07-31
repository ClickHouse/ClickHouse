#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>

using namespace DB;

namespace
{

using FileStatus = ObjectStorageQueueIFileMetadata::FileStatus;
using FileStatusPtr = ObjectStorageQueueIFileMetadata::FileStatusPtr;

/// `afterSetProcessing` does not touch keeper, so the state machine of a shared
/// `FileStatus` can be tested on a metadata object with dummy keeper paths.
std::shared_ptr<ObjectStorageQueueUnorderedFileMetadata> makeFileMetadata(
    FileStatusPtr file_status, std::atomic<size_t> & metadata_ref_count)
{
    return std::make_shared<ObjectStorageQueueUnorderedFileMetadata>(
        "/clickhouse/test_foreign_processing",
        "data/file.csv",
        file_status,
        /* max_loading_retries */ 3,
        metadata_ref_count,
        /* use_persistent_processing_nodes */ false,
        "default",
        getLogger("gtest_file_status_foreign_processing"));
}

}

/// Losing the race for the `processing` node to another processor must cache the
/// `Processing` state only as a non-terminal observation of the foreign node.
TEST(ObjectStorageQueueFileStatus, ForeignProcessingNodeIsNotTerminal)
{
    auto file_status = std::make_shared<FileStatus>("data/file.csv");
    std::atomic<size_t> metadata_ref_count{0};
    auto metadata = makeFileMetadata(file_status, metadata_ref_count);

    metadata->afterSetProcessing(/* success */ false, FileStatus::State::Processing);

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_TRUE(file_status->isProcessingByAnotherProcessor());
    /// A fresh observation is trusted: the file is not retried until the observation expires.
    ASSERT_FALSE(file_status->isProcessingRetryable());
}

/// A same-server contender which loses the race to a concurrent local processor
/// (the `FileStatus` is shared between tables on this server which use the same keeper
/// path, and between insert threads) must not relabel the locally-owned `Processing`
/// state as "processing by another processor".
TEST(ObjectStorageQueueFileStatus, LocalProcessingStateIsPreservedOnSameServerContention)
{
    auto file_status = std::make_shared<FileStatus>("data/file.csv");
    std::atomic<size_t> metadata_ref_count{0};

    /// The local owner has successfully created the `processing` node.
    auto owner = makeFileMetadata(file_status, metadata_ref_count);
    file_status->onProcessing();

    /// A stale contender sharing the same `FileStatus` loses on the owner's node.
    auto contender = makeFileMetadata(file_status, metadata_ref_count);
    contender->afterSetProcessing(/* success */ false, FileStatus::State::Processing);

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_FALSE(file_status->isProcessingByAnotherProcessor());
    ASSERT_FALSE(file_status->isProcessingRetryable());
}

/// Once the foreign observation is replaced by a real state transition,
/// the "processing by another processor" hint must be cleared.
TEST(ObjectStorageQueueFileStatus, ForeignProcessingHintIsCleared)
{
    auto file_status = std::make_shared<FileStatus>("data/file.csv");
    std::atomic<size_t> metadata_ref_count{0};
    auto metadata = makeFileMetadata(file_status, metadata_ref_count);

    metadata->afterSetProcessing(/* success */ false, FileStatus::State::Processing);
    ASSERT_TRUE(file_status->isProcessingByAnotherProcessor());

    /// The foreign processor committed the file: the next attempt finds the `failed` node.
    metadata->afterSetProcessing(/* success */ false, FileStatus::State::Failed);
    ASSERT_EQ(file_status->state.load(), FileStatus::State::Failed);
    ASSERT_FALSE(file_status->isProcessingByAnotherProcessor());

    metadata->afterSetProcessing(/* success */ false, FileStatus::State::Processing);
    ASSERT_TRUE(file_status->isProcessingByAnotherProcessor());

    /// This processor took the file over: the state is ours again.
    file_status->onProcessing();
    ASSERT_FALSE(file_status->isProcessingByAnotherProcessor());
}
