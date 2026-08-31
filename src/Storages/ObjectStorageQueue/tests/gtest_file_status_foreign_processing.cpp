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
    if constexpr (requires { typename Metadata::ForeignProcessingObservers; })
    {
        /// Keep the registry shared by the metadata objects in this test. A contender
        /// must observe the same foreign node as the processor which first saw it.
        static const auto foreign_processing_observers = std::make_shared<typename Metadata::ForeignProcessingObservers>(100);

        return std::make_shared<Metadata>(
            "/clickhouse/test_foreign_processing",
            "data/file.csv",
            file_status,
            /* max_loading_retries */ 3,
            metadata_ref_count,
            /* use_persistent_processing_nodes */ false,
            "default",
            getLogger("gtest_file_status_foreign_processing"),
            /* foreign_processing_node_cache_ttl_sec */ 0,
            foreign_processing_observers);
    }
    else
    {
        return std::make_shared<Metadata>(
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
template <typename Metadata = ObjectStorageQueueUnorderedFileMetadata>
void expectForeignProcessingHintIsClearedByLocalProcessing()
{
    if constexpr (requires(typename Metadata::FileStatus & status) { status.isProcessingByAnotherProcessor(); })
    {
        /// The dependent alias keeps this branch uninstantiated at the merge base.
        using FS = typename Metadata::FileStatus;

        auto file_status = std::make_shared<FS>("data/file.csv");
        std::atomic<size_t> metadata_ref_count{0};
        auto metadata = makeFileMetadata<Metadata>(file_status, metadata_ref_count);

        metadata->afterSetProcessing(/* success */ false, FS::State::Processing);
        ASSERT_EQ(file_status->state.load(), FS::State::Processing);

        /// This processor took the file over: the state is ours again.
        file_status->onProcessing();
        file_status->processed_rows = 5;
        ASSERT_FALSE(file_status->isProcessingByAnotherProcessor());

        auto contender = makeFileMetadata<Metadata>(file_status, metadata_ref_count);
        contender->afterSetProcessing(/* success */ false, FS::State::Processing);

        ASSERT_EQ(file_status->state.load(), FS::State::Processing);
        ASSERT_EQ(file_status->processed_rows.load(), 5UL);
    }
    else
        FAIL() << "FileStatus does not distinguish local and foreign processing states";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingHintIsClearedByLocalProcessing)
{
    expectForeignProcessingHintIsClearedByLocalProcessing();
}

/// A foreign `processing` observation belongs to the table which made it. A table
/// which has not observed the node itself must probe keeper instead of inheriting
/// another table's cache deadline.
template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectForeignProcessingCacheDeadlineIsPerTable()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        /// The dependent aliases keep this branch uninstantiated at the merge base.
        using FS = typename Meta::FileStatus;
        using Observers = typename Meta::ForeignProcessingObservers;

        auto file_status = std::make_shared<FS>("data/file.csv");
        Observers first_observers(1);
        Observers second_observers(1);

        file_status->onProcessingByAnotherProcessor(first_observers);

        ASSERT_FALSE(file_status->shouldRetryProcessing(first_observers, 3600));
        ASSERT_TRUE(file_status->shouldRetryProcessing(second_observers, 3600));
        ASSERT_NE(file_status->processingByAnotherProcessorSince(first_observers), 0);
        ASSERT_EQ(file_status->processingByAnotherProcessorSince(second_observers), 0);
    }
    else
        FAIL() << "FileStatus does not keep foreign processing observations per table";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingCacheDeadlineIsPerTable)
{
    expectForeignProcessingCacheDeadlineIsPerTable();
}

/// An observation describes one foreign hold of a path. If the file stops being processed
/// by another processor and is held again later, a table which observed only the earlier
/// hold must check keeper instead of reusing its old deadline: the new owner may have
/// released the file already.
template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectStaleObservationIsNotReusedForALaterForeignHold()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        using FS = typename Meta::FileStatus;
        using Observers = typename Meta::ForeignProcessingObservers;

        auto file_status = std::make_shared<FS>("data/file.csv");
        Observers first_table_observers(10);
        Observers second_table_observers(10);

        /// The first table observes a foreign hold of the file.
        file_status->onProcessingByAnotherProcessor(first_table_observers);
        ASSERT_FALSE(file_status->shouldRetryProcessing(first_table_observers, 3600));

        /// The hold is over: the file was released and reset.
        (*file_status).reset();
        ASSERT_FALSE(file_status->isProcessingByAnotherProcessor());

        /// The second table is the first to observe a new foreign hold of the same path.
        file_status->onProcessingByAnotherProcessor(second_table_observers);

        ASSERT_EQ(file_status->processingByAnotherProcessorSince(first_table_observers), 0);
        ASSERT_TRUE(file_status->shouldRetryProcessing(first_table_observers, 3600));
        ASSERT_FALSE(file_status->shouldRetryProcessing(second_table_observers, 3600));
    }
    else
        FAIL() << "FileStatus does not invalidate observations of a previous foreign hold";
}

TEST(ObjectStorageQueueFileStatus, StaleObservationIsNotReusedForALaterForeignHold)
{
    expectStaleObservationIsNotReusedForALaterForeignHold();
}

template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectForeignProcessingObserversEvictOnlyTheLeastRecentlyUsedPath()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        typename Meta::ForeignProcessingObservers observers(2);

        observers.set("data/first.csv", /* generation */ 1, 1);
        observers.set("data/second.csv", /* generation */ 1, 2);
        ASSERT_EQ(observers.get("data/first.csv", 1), 1);

        observers.set("data/third.csv", /* generation */ 1, 3);
        ASSERT_EQ(observers.get("data/first.csv", 1), 1);
        ASSERT_EQ(observers.get("data/second.csv", 1), 0);
        ASSERT_EQ(observers.get("data/third.csv", 1), 3);
    }
    else
        FAIL() << "There is no registry of foreign processing observations";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingObserversEvictOnlyTheLeastRecentlyUsedPath)
{
    expectForeignProcessingObserversEvictOnlyTheLeastRecentlyUsedPath();
}

template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectForeignProcessingObserversFollowChangedCapacity()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        typename Meta::ForeignProcessingObservers observers(1);

        observers.set("data/first.csv", /* generation */ 1, 1);
        observers.setMaxEntries(2);
        observers.set("data/second.csv", /* generation */ 1, 2);
        ASSERT_EQ(observers.get("data/first.csv", 1), 1);
        ASSERT_EQ(observers.get("data/second.csv", 1), 2);

        observers.setMaxEntries(1);
        ASSERT_EQ(observers.get("data/first.csv", 1), 0);
        ASSERT_EQ(observers.get("data/second.csv", 1), 2);
    }
    else
        FAIL() << "There is no registry of foreign processing observations";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingObserversFollowChangedCapacity)
{
    expectForeignProcessingObserversFollowChangedCapacity();
}

template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectForeignProcessingObserversAllowUnlimitedEntries()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        typename Meta::ForeignProcessingObservers observers(0);

        observers.set("data/first.csv", /* generation */ 1, 1);
        observers.set("data/second.csv", /* generation */ 1, 2);
        observers.setMaxEntries(0);
        observers.set("data/third.csv", /* generation */ 1, 3);

        ASSERT_EQ(observers.get("data/first.csv", 1), 1);
        ASSERT_EQ(observers.get("data/second.csv", 1), 2);
        ASSERT_EQ(observers.get("data/third.csv", 1), 3);
    }
    else
        FAIL() << "There is no registry of foreign processing observations";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingObserversAllowUnlimitedEntries)
{
    expectForeignProcessingObserversAllowUnlimitedEntries();
}

/// The registry must stay under the same memory contract as the metadata cache: the byte
/// limit bounds it even when the number of entries is unlimited.
template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectForeignProcessingObserversAreBoundedByBytes()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        static constexpr size_t observations_count = 100;
        const auto path_of = [](size_t i) { return "data/" + std::to_string(1000 + i) + ".csv"; };

        typename Meta::ForeignProcessingObservers observers(/* max_entries */ 0, /* max_bytes */ 0);
        for (size_t i = 0; i < observations_count; ++i)
            observers.set(path_of(i), /* generation */ 1, static_cast<time_t>(i + 1));

        const size_t size_of_all = observers.sizeInBytes();
        ASSERT_GT(size_of_all, 0);

        /// Lowering the limit evicts the least recently used entries immediately.
        observers.setMaxSizeInBytes(size_of_all / 2);
        ASSERT_LE(observers.sizeInBytes(), size_of_all / 2);
        ASSERT_GT(observers.count(), 0);
        ASSERT_LT(observers.count(), observations_count);
        ASSERT_EQ(observers.get(path_of(0), 1), 0);
        ASSERT_GT(observers.get(path_of(observations_count - 1), 1), 0);

        /// The limit keeps holding while new observations arrive.
        for (size_t i = 0; i < observations_count; ++i)
            observers.set(path_of(observations_count + i), /* generation */ 1, static_cast<time_t>(i + 1));

        ASSERT_LE(observers.sizeInBytes(), size_of_all / 2);
        ASSERT_GT(observers.count(), 0);
    }
    else
        FAIL() << "There is no registry of foreign processing observations";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingObserversAreBoundedByBytes)
{
    expectForeignProcessingObserversAreBoundedByBytes();
}

/// Evicting the entries is not enough: an `std::unordered_map` keeps its peak bucket array,
/// so the registry of a long-lived table would stay at its high-water mark even after the
/// limits were lowered. The memory must be given back.
template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectForeignProcessingObserversReclaimTheBucketArray()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        static constexpr size_t observations_count = 10000;

        typename Meta::ForeignProcessingObservers observers(/* max_entries */ 0, /* max_bytes */ 0);
        for (size_t i = 0; i < observations_count; ++i)
            observers.set("data/" + std::to_string(1000 + i) + ".csv", /* generation */ 1, static_cast<time_t>(i + 1));

        const size_t size_of_all = observers.sizeInBytes();
        observers.setMaxEntries(1);
        ASSERT_EQ(observers.count(), 1);
        /// The remaining entry is one of ten thousand, so nothing close to the peak may be left.
        ASSERT_LT(observers.sizeInBytes(), size_of_all / 100);
    }
    else
        FAIL() << "There is no registry of foreign processing observations";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingObserversReclaimTheBucketArray)
{
    expectForeignProcessingObserversReclaimTheBucketArray();
}

/// An observation of a path made for an earlier foreign hold must not be returned for a
/// later one, even when the entry is still in the registry.
template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectForeignProcessingObserversAreScopedToTheGeneration()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        typename Meta::ForeignProcessingObservers observers(10);

        observers.set("data/first.csv", /* generation */ 1, 100);
        ASSERT_EQ(observers.get("data/first.csv", 1), 100);
        ASSERT_EQ(observers.get("data/first.csv", 2), 0);

        observers.set("data/first.csv", /* generation */ 2, 200);
        ASSERT_EQ(observers.get("data/first.csv", 2), 200);
        ASSERT_EQ(observers.get("data/first.csv", 1), 0);
    }
    else
        FAIL() << "There is no registry of foreign processing observations";
}

TEST(ObjectStorageQueueFileStatus, ForeignProcessingObserversAreScopedToTheGeneration)
{
    expectForeignProcessingObserversAreScopedToTheGeneration();
}

/// The pre-Keeper state gate must keep a locally owned `Processing` state terminal.
/// A foreign state without an observation for the asking table, on the other hand,
/// must be retried so that the table can check whether the foreign node was released.
template <typename Meta = ObjectStorageQueueIFileMetadata>
void expectOnlyForeignProcessingIsRetryable()
{
    if constexpr (requires { typename Meta::ForeignProcessingObservers; })
    {
        /// The dependent aliases keep this branch uninstantiated at the merge base.
        using FS = typename Meta::FileStatus;
        using Observers = typename Meta::ForeignProcessingObservers;

        auto file_status = std::make_shared<FS>("data/file.csv");
        Observers observing_observers(1);
        Observers other_observers(1);

        file_status->onProcessing();
        ASSERT_FALSE(file_status->shouldRetryProcessing(observing_observers, time_t{}));

        file_status->onProcessingByAnotherProcessor(observing_observers);
        ASSERT_TRUE(file_status->shouldRetryProcessing(other_observers, 3600));
    }
    else
        FAIL() << "FileStatus does not distinguish local and foreign processing states";
}

TEST(ObjectStorageQueueFileStatus, OnlyForeignProcessingIsRetryable)
{
    expectOnlyForeignProcessingIsRetryable();
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
