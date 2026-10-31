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
template <typename Metadata = ObjectStorageQueueUnorderedFileMetadata>
std::shared_ptr<Metadata> makeFileMetadata(
    FileStatusPtr file_status, std::atomic<size_t> & metadata_ref_count)
{
    /// Zero: a `Processing` state observed in keeper is never trusted, keeper is always asked.
    static const std::atomic<size_t> processing_state_cache_ttl_seconds{0};

    return std::make_shared<Metadata>(
        "/clickhouse/test_foreign_processing",
        "data/file.csv",
        file_status,
        /* max_loading_retries */ 3,
        metadata_ref_count,
        /* use_persistent_processing_nodes */ false,
        processing_state_cache_ttl_seconds,
        "default",
        getLogger("gtest_file_status_foreign_processing"));
}

/// Mirrors what `trySetProcessing` / `prepareSetProcessingRequests` do before probing
/// keeper: remember which terminal state the shared record had, so that the foreign
/// `processing` observation can be recognised as older than a terminal state committed
/// by another processor in the meantime.
template <typename Metadata>
void snapshotTerminalStateGeneration(Metadata & metadata)
{
    if constexpr (requires { metadata.snapshotTerminalStateGeneration(); })
        metadata.snapshotTerminalStateGeneration();
}

}

/// A same-server contender which loses the race to a concurrent local processor
/// (the `FileStatus` is shared between tables and threads) must not relabel the locally
/// owned `Processing` state as observed in keeper: the owner updates the record itself,
/// and a false marker would make the other tables of this server treat the file as held
/// by another server (recheck it after the TTL, and block its ordering domain).
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
    snapshotTerminalStateGeneration(*contender);
    contender->afterSetProcessing(/* success */ false, FileStatus::State::Processing);

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_EQ(file_status->processed_rows.load(), 7UL);
    ASSERT_EQ(file_status->processing_observed_in_keeper_time.load(), 0);
}

/// Taking the file over locally must clear the "observed in keeper" marker, otherwise a
/// later local contender would consider the state foreign and mark it as such again.
TEST(ObjectStorageQueueFileStatus, ForeignProcessingHintIsClearedByLocalProcessing)
{
    auto file_status = std::make_shared<FileStatus>("data/file.csv");
    std::atomic<size_t> metadata_ref_count{0};
    auto metadata = makeFileMetadata(file_status, metadata_ref_count);

    snapshotTerminalStateGeneration(*metadata);
    metadata->afterSetProcessing(/* success */ false, FileStatus::State::Processing);
    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_NE(file_status->processing_observed_in_keeper_time.load(), 0);

    /// This processor took the file over: the state is ours again.
    file_status->onProcessing();
    file_status->processed_rows = 5;
    ASSERT_EQ(file_status->processing_observed_in_keeper_time.load(), 0);

    auto contender = makeFileMetadata(file_status, metadata_ref_count);
    snapshotTerminalStateGeneration(*contender);
    contender->afterSetProcessing(/* success */ false, FileStatus::State::Processing);

    ASSERT_EQ(file_status->state.load(), FileStatus::State::Processing);
    ASSERT_EQ(file_status->processed_rows.load(), 5UL);
    ASSERT_EQ(file_status->processing_observed_in_keeper_time.load(), 0);
}

namespace
{

/// Only a `Processing` state observed in keeper is trusted for the cache TTL, and a zero
/// TTL trusts nothing. A locally owned `Processing` state is never "trusted" in this
/// sense: its owner updates it, so there is nothing to recheck.
template <typename FS>
void expectOnlyAnObservedProcessingStateIsTrustedForTheTTL()
{
    if constexpr (requires(const FS & fs) { fs.isProcessingObservedInKeeperTrusted(size_t{}); })
    {
        auto file_status = std::make_shared<FS>("data/file.csv");

        file_status->onProcessing();
        ASSERT_FALSE(file_status->isProcessingObservedInKeeper());
        ASSERT_FALSE(file_status->isProcessingObservedInKeeperTrusted(3600));

        file_status->onStateObservedInKeeper(FS::State::Processing);
        ASSERT_TRUE(file_status->isProcessingObservedInKeeper());
        ASSERT_TRUE(file_status->isProcessingObservedInKeeperTrusted(3600));
        ASSERT_FALSE(file_status->isProcessingObservedInKeeperTrusted(0));

        /// A terminal state committed elsewhere ends the observation.
        file_status->onStateObservedInKeeper(FS::State::Processed);
        ASSERT_FALSE(file_status->isProcessingObservedInKeeper());
        ASSERT_FALSE(file_status->isProcessingObservedInKeeperTrusted(3600));
    }
    else
        FAIL() << "FileStatus cannot tell whether an observed `Processing` state is still trusted";
}

}

TEST(ObjectStorageQueueFileStatus, OnlyAnObservedProcessingStateIsTrustedForTheTTL)
{
    expectOnlyAnObservedProcessingStateIsTrustedForTheTTL<FileStatus>();
}

namespace
{

/// A terminal state committed by another processor replaces the data of an abandoned
/// local attempt: `Processed` must not keep a stale exception, and `Failed` must carry
/// the exception of the processor which actually failed the file.
template <typename FS>
void expectForeignTerminalStateReplacesDataOfPreviousLocalAttempt()
{
    if constexpr (requires(FS fs) { fs.onTerminalStateObservedInKeeper(FS::State::Processed, std::string{}, size_t{}); })
    {
        auto file_status = std::make_shared<FS>("data/file.csv");

        file_status->onProcessing();
        file_status->processed_rows = 10;
        file_status->retries = 2;
        file_status->onFailed("Cannot read the file");

        /// Another processor has committed the file as processed.
        file_status->onTerminalStateObservedInKeeper(FS::State::Processed, "", /* retries_ */ 0);

        ASSERT_EQ(file_status->state.load(), FS::State::Processed);
        ASSERT_EQ(file_status->processed_rows.load(), 0UL);
        ASSERT_EQ(file_status->processing_start_time.load(), 0);
        ASSERT_EQ(file_status->processing_end_time.load(), 0);
        ASSERT_EQ(file_status->getException(), "");
        ASSERT_FALSE(file_status->isProcessingObservedInKeeper());

        /// Another processor has failed the file: its exception and retries are reported.
        file_status->onProcessing();
        file_status->processed_rows = 5;
        file_status->onTerminalStateObservedInKeeper(FS::State::Failed, "Cannot parse the file", /* retries_ */ 3);

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
            snapshotTerminalStateGeneration(*metadata);
            metadata->afterSetProcessing(/* success */ false, FS::State::Processing);
            ASSERT_TRUE(file_status->isProcessingObservedInKeeper());

            snapshotTerminalStateGeneration(*metadata);
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

            snapshotTerminalStateGeneration(*metadata);
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

            snapshotTerminalStateGeneration(*metadata);
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

/// A cached `Failed` state of a retriable local attempt (`retries < loading_retries`) does
/// not describe the terminal `failed` node written later by another processor which
/// exhausted the retries: the equal-state guard must not keep the stale local record.
template <typename Metadata>
void expectTerminalFailureReplacesCachedRetriableLocalFailure()
{
    if constexpr (requires { typename Metadata::FileTerminalState; })
    {
        /// The dependent alias keeps this branch uninstantiated at the merge base.
        using FS = typename Metadata::FileStatus;
        using TerminalState = typename Metadata::FileTerminalState;
        std::atomic<size_t> metadata_ref_count{0};

        {
            /// A retriable local failure, then another processor exhausts the retries.
            auto file_status = std::make_shared<FS>("data/file.csv");
            auto metadata = makeFileMetadata(file_status, metadata_ref_count);

            file_status->onProcessing();
            file_status->retries = 1;
            file_status->onFailed("Retriable local failure");

            snapshotTerminalStateGeneration(*metadata);
            metadata->afterSetProcessing(
                /* success */ false,
                FS::State::Failed,
                TerminalState{FS::State::Failed, "Terminal foreign failure", 3});

            ASSERT_EQ(file_status->state.load(), FS::State::Failed);
            ASSERT_EQ(file_status->getException(), "Terminal foreign failure");
            ASSERT_EQ(file_status->retries.load(), 3UL);
        }

        {
            /// The local attempt was the terminal one: its record (with the per-attempt
            /// data such as the processing times) is kept.
            auto file_status = std::make_shared<FS>("data/file.csv");
            auto metadata = makeFileMetadata(file_status, metadata_ref_count);

            file_status->onProcessing();
            file_status->retries = 3;
            file_status->onFailed("Terminal local failure");
            const auto end_time = file_status->processing_end_time.load();
            ASSERT_NE(end_time, 0);

            snapshotTerminalStateGeneration(*metadata);
            metadata->afterSetProcessing(
                /* success */ false,
                FS::State::Failed,
                TerminalState{FS::State::Failed, "Terminal local failure", 3});

            ASSERT_EQ(file_status->state.load(), FS::State::Failed);
            ASSERT_EQ(file_status->getException(), "Terminal local failure");
            ASSERT_EQ(file_status->processing_end_time.load(), end_time);
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

TEST(ObjectStorageQueueFileStatus, TerminalFailureReplacesCachedRetriableLocalFailure)
{
    expectTerminalFailureReplacesCachedRetriableLocalFailure<ObjectStorageQueueUnorderedFileMetadata>();
}

namespace
{

/// Two tables sharing one `keeper_path` share one `FileStatus`. One of them loses the race
/// for the `processing` node, while the other discovers, in the meantime, that the file has
/// already been committed. The loser must not downgrade the cached terminal record back to
/// `Processing`: that would make `system.s3queue_metadata_cache` stale again and, in
/// `ordered` mode, keep the ordering domain blocked until the observation expires.
template <typename Metadata>
void expectTerminalStateIsNotClobberedByALoserOfSetProcessing()
{
    if constexpr (requires { typename Metadata::FileTerminalState; })
    {
        /// The dependent aliases keep this branch uninstantiated at the merge base.
        using FS = typename Metadata::FileStatus;
        using TerminalState = typename Metadata::FileTerminalState;
        std::atomic<size_t> metadata_ref_count{0};

        auto file_status = std::make_shared<FS>("data/file.csv");
        auto loser = makeFileMetadata(file_status, metadata_ref_count);
        auto observer = makeFileMetadata(file_status, metadata_ref_count);

        /// The loser reads keeper and finds the `processing` node of another processor...
        snapshotTerminalStateGeneration(*loser);

        /// ... but before it writes that observation back, the other table of this server
        /// discovers the `processed` node committed by that processor and refreshes the
        /// shared record.
        snapshotTerminalStateGeneration(*observer);
        observer->afterSetProcessing(
            /* success */ false,
            FS::State::Processed,
            TerminalState{FS::State::Processed});
        ASSERT_EQ(file_status->state.load(), FS::State::Processed);

        loser->afterSetProcessing(/* success */ false, FS::State::Processing);

        ASSERT_EQ(file_status->state.load(), FS::State::Processed);
        ASSERT_FALSE(file_status->isProcessingObservedInKeeper());

        /// A foreign hold observed after that terminal state is a later fact, and is recorded.
        auto contender = makeFileMetadata(file_status, metadata_ref_count);
        snapshotTerminalStateGeneration(*contender);
        contender->afterSetProcessing(/* success */ false, FS::State::Processing);

        ASSERT_EQ(file_status->state.load(), FS::State::Processing);
        ASSERT_TRUE(file_status->isProcessingObservedInKeeper());
    }
    else
        FAIL() << "A loser of the set-processing race can clobber a terminal cached state";
}

}

TEST(ObjectStorageQueueFileStatus, TerminalStateIsNotClobberedByALoserOfSetProcessing)
{
    expectTerminalStateIsNotClobberedByALoserOfSetProcessing<ObjectStorageQueueUnorderedFileMetadata>();
}
