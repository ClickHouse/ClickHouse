#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>

using namespace DB;

/// NOTE: the `Bugfix validation (unit tests)` CI job compiles this file against the
/// merge base (without the fix) and expects it to fail at runtime there. Therefore the
/// members added by the fix are referenced only from discarded-unless-present branches
/// of `if constexpr (requires ...)` inside templates: without the fix the file still
/// compiles, and the corresponding tests fail at runtime.

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

/// Observing the `processing` node of another processor after a local failure must not keep
/// the per-attempt data of that failure, otherwise introspection reports a `Processing`
/// file with a stale exception. The number of retries must be preserved.
TEST(ObjectStorageQueueFileStatus, ForeignProcessingResetsDataOfPreviousLocalAttempt)
{
    auto file_status = std::make_shared<FileStatus>("data/file.csv");
    std::atomic<size_t> metadata_ref_count{0};
    auto metadata = makeFileMetadata(file_status, metadata_ref_count);

    file_status->onProcessing();
    file_status->processed_rows = 10;
    file_status->retries = 2;
    file_status->onFailed("Cannot read the file");

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Failed);
    ASSERT_NE(file_status->processing_end_time.load(), 0);

    metadata->afterSetProcessing(/* success */ false, FileStatus::State::Processing);

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_EQ(file_status->processed_rows.load(), 0UL);
    ASSERT_EQ(file_status->processing_end_time.load(), 0);
    ASSERT_EQ(file_status->getException(), "");
    ASSERT_EQ(file_status->retries.load(), 2UL);
}

/// A same-server contender which loses the race to a concurrent local processor
/// (the `FileStatus` is shared between tables and threads) must not relabel
/// the locally owned `Processing` state as "processing by another processor",
/// which is observable as the data of the ongoing local attempt being dropped.
TEST(ObjectStorageQueueFileStatus, LocalProcessingStateIsPreservedOnSameServerContention)
{
    auto file_status = std::make_shared<FileStatus>("data/file.csv");
    std::atomic<size_t> metadata_ref_count{0};

    /// The local owner has successfully created the `processing` node.
    auto owner = makeFileMetadata(file_status, metadata_ref_count);
    file_status->onProcessing();
    file_status->processed_rows = 7;

    /// A stale contender sharing the same `FileStatus` loses on the owner's node.
    auto contender = makeFileMetadata(file_status, metadata_ref_count);
    contender->afterSetProcessing(/* success */ false, FileStatus::State::Processing);

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_EQ(file_status->processed_rows.load(), 7UL);
}

/// Taking the file over locally must clear the "processing by another processor" hint,
/// otherwise a later local contender would consider the state foreign and drop the data
/// of the ongoing local attempt.
TEST(ObjectStorageQueueFileStatus, ForeignProcessingHintIsClearedByLocalProcessing)
{
    auto file_status = std::make_shared<FileStatus>("data/file.csv");
    std::atomic<size_t> metadata_ref_count{0};
    auto metadata = makeFileMetadata(file_status, metadata_ref_count);

    metadata->afterSetProcessing(/* success */ false, FileStatus::State::Processing);
    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);

    /// This processor took the file over: the state is ours again.
    file_status->onProcessing();
    file_status->processed_rows = 5;

    auto contender = makeFileMetadata(file_status, metadata_ref_count);
    contender->afterSetProcessing(/* success */ false, FileStatus::State::Processing);

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_EQ(file_status->processed_rows.load(), 5UL);
}

namespace
{

/// A terminal state committed by another processor replaces the data of an abandoned
/// local attempt: `Processed` must not keep a stale exception, and `Failed` must carry
/// the exception of the processor which actually failed the file.
template <typename FS>
void expectForeignTerminalStateReplacesDataOfPreviousLocalAttempt()
{
    if constexpr (requires(FS fs) { fs.onTerminalStateByAnotherProcessor(FS::State::Processed, std::string{}, size_t{}); })
    {
        auto file_status = std::make_shared<FS>("data/file.csv");

        file_status->onProcessing();
        file_status->processed_rows = 10;
        file_status->retries = 2;
        file_status->onFailed("Cannot read the file");

        /// Another processor has committed the file as processed.
        file_status->onTerminalStateByAnotherProcessor(FS::State::Processed, "", /* retries_ */ 0);

        ASSERT_EQ(file_status->state.load(), FS::State::Processed);
        ASSERT_EQ(file_status->processed_rows.load(), 0UL);
        ASSERT_EQ(file_status->processing_start_time.load(), 0);
        ASSERT_EQ(file_status->processing_end_time.load(), 0);
        ASSERT_EQ(file_status->getException(), "");
        ASSERT_FALSE(file_status->isProcessingByAnotherProcessor());

        /// Another processor has failed the file: its exception and retries are reported.
        file_status->onProcessing();
        file_status->processed_rows = 5;
        file_status->onTerminalStateByAnotherProcessor(FS::State::Failed, "Cannot parse the file", /* retries_ */ 3);

        ASSERT_EQ(file_status->state.load(), FS::State::Failed);
        ASSERT_EQ(file_status->processed_rows.load(), 0UL);
        ASSERT_EQ(file_status->getException(), "Cannot parse the file");
        ASSERT_EQ(file_status->retries.load(), 3UL);
    }
    else
        FAIL() << "FileStatus does not keep track of terminal states committed by another processor";
}

/// A terminal node discovered by the set-processing probe (`trySetProcessing` /
/// `prepareSetProcessingRequests`) must refresh the whole cached record, with the same
/// guards as the listing pre-filter: a record which already describes this terminal
/// state (a local attempt) and a locally owned `Processing` state are kept.
template <typename Metadata>
void expectSetProcessingTerminalDiscoveryRefreshesWholeRecord()
{
    if constexpr (requires { typename Metadata::FileTerminalState; })
    {
        /// The dependent alias keeps this branch uninstantiated at the merge base.
        using FS = typename Metadata::FileStatus;
        using TerminalState = typename Metadata::FileTerminalState;
        std::atomic<size_t> metadata_ref_count{0};

        {
            /// The file was failed by another processor after an abandoned local attempt:
            /// the cached record follows the `failed` node instead of mixing in stale data.
            auto file_status = std::make_shared<FS>("data/file.csv");
            auto metadata = makeFileMetadata(file_status, metadata_ref_count);

            file_status->onProcessing();
            file_status->processed_rows = 10;
            file_status->onFailed("Cannot read the file");
            metadata->afterSetProcessing(/* success */ false, FS::State::Processing);
            ASSERT_TRUE(file_status->isProcessingByAnotherProcessor());

            metadata->afterSetProcessing(
                /* success */ false,
                FS::State::Failed,
                TerminalState{FS::State::Failed, "Cannot parse the file", 3});

            ASSERT_EQ(file_status->state.load(), FS::State::Failed);
            ASSERT_EQ(file_status->processed_rows.load(), 0UL);
            ASSERT_EQ(file_status->processing_start_time.load(), 0);
            ASSERT_EQ(file_status->processing_end_time.load(), 0);
            ASSERT_EQ(file_status->getException(), "Cannot parse the file");
            ASSERT_EQ(file_status->retries.load(), 3UL);
        }

        {
            /// The cached record of a file processed by THIS server is kept when its own
            /// `processed` node is rediscovered by a later set-processing attempt.
            auto file_status = std::make_shared<FS>("data/file.csv");
            auto metadata = makeFileMetadata(file_status, metadata_ref_count);

            file_status->onProcessing();
            file_status->processed_rows = 7;
            file_status->setProcessingEndTime();
            file_status->onProcessed();

            metadata->afterSetProcessing(
                /* success */ false,
                FS::State::Processed,
                TerminalState{FS::State::Processed});

            ASSERT_EQ(file_status->state.load(), FS::State::Processed);
            ASSERT_EQ(file_status->processed_rows.load(), 7UL);
        }

        {
            /// A locally owned `Processing` state is updated by its owner on commit.
            auto file_status = std::make_shared<FS>("data/file.csv");
            auto metadata = makeFileMetadata(file_status, metadata_ref_count);

            file_status->onProcessing();
            file_status->processed_rows = 5;

            metadata->afterSetProcessing(
                /* success */ false,
                FS::State::Processed,
                TerminalState{FS::State::Processed});

            ASSERT_EQ(file_status->state.load(), FS::State::Processing);
            ASSERT_EQ(file_status->processed_rows.load(), 5UL);
        }
    }
    else
        FAIL() << "The set-processing probe does not report the discovered terminal node metadata";
}

}

TEST(ObjectStorageQueueFileStatus, ForeignTerminalStateReplacesDataOfPreviousLocalAttempt)
{
    expectForeignTerminalStateReplacesDataOfPreviousLocalAttempt<FileStatus>();
}

TEST(ObjectStorageQueueFileStatus, SetProcessingTerminalDiscoveryRefreshesWholeRecord)
{
    expectSetProcessingTerminalDiscoveryRefreshesWholeRecord<ObjectStorageQueueUnorderedFileMetadata>();
}
