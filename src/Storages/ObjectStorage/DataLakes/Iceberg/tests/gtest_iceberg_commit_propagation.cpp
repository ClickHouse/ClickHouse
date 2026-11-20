#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <cerrno>
#include <functional>
#include <thread>

#include <base/scope_guard.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/ErrnoException.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Common/WriteOutcome.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#if USE_AWS_S3
#include <IO/S3Common.h>
#endif

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int READONLY;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
    extern const int S3_ERROR;
}

namespace
{

using WriteErrorThrower = std::function<void(const std::string & path)>;
using ExistsAnswer = std::function<bool(size_t call)>;
/// The outcome that the storage reports for the failed write. Empty: its `writeObject` fails at once, so nothing is
/// sent.
using StorageOutcome = std::optional<WriteOutcome>;

/// The buffer of a storage: its write fails with the error that the test supplies, and it reports `outcome`.
class FailingWriteBuffer : public WriteBufferFromFileBase
{
public:
    FailingWriteBuffer(WriteErrorThrower throw_write_error_, std::string path_, std::optional<WriteOutcome> outcome_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , throw_write_error(std::move(throw_write_error_))
        , path(std::move(path_))
        , outcome(outcome_)
    {
    }

    void sync() override { }
    std::string getFileName() const override { return path; }
    std::optional<WriteOutcome> getWriteOutcome() const override { return outcome; }

private:
    void nextImpl() override { throw_write_error(path); }

    WriteErrorThrower throw_write_error;
    std::string path;
    std::optional<WriteOutcome> outcome;
};

/// `writeMetadataFileAndVersionHint` reports a commit that published nothing as `false`, which every caller reads as
/// a lost compare-and-swap and retries after removing the files of the attempt. A write the storage refused (it cannot
/// express the condition, or it denies the write) fails the same way on every retry, so it has to propagate instead of
/// being reported as a lost race. A write that was sent and failed without proof either way may have published the
/// new version, so it throws `UNKNOWN_STATUS_OF_TRANSACTION`, and the caller keeps the files. This stub makes these
/// outcomes observable without an object storage: `exists` answers what the test supplies (absent by default, so the
/// function proceeds to the write), and `writeObject` throws the error the test supplies. Everything else throws, so
/// an unexpected call is loud rather than silently absorbed.
class ThrowingObjectStorage : public IObjectStorage
{
public:
    ThrowingObjectStorage(WriteErrorThrower throw_write_error_, ExistsAnswer exists_answer_, StorageOutcome storage_outcome_)
        : throw_write_error(std::move(throw_write_error_))
        , exists_answer(std::move(exists_answer_))
        , storage_outcome(storage_outcome_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode,
        std::optional<ObjectAttributes>,
        size_t,
        const WriteSettings & write_settings) override
    {
        /// The commit must ask for the metadata file to be created exclusively: without the
        /// condition the write is a plain overwrite and one of two concurrent writers is lost. Only
        /// the metadata write reaches this stub, because every test leaves the commit before the
        /// version-hint write. `EXPECT_*` keeps the throw below reachable.
        EXPECT_EQ(write_settings.object_storage_write_if_none_match, "*");
        EXPECT_TRUE(write_settings.object_storage_write_if_match.empty());

        if (storage_outcome)
            return std::make_unique<FailingWriteBuffer>(throw_write_error, object.remote_path, *storage_outcome);

        throw_write_error(object.remote_path);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The stub must throw");
    }

    /// Call 0 is the commit's pre-check (absent, so the write is attempted), later calls the check after a refusal.
    bool exists(const StoredObject &) const override { return exists_answer(exists_calls++); }

    std::string getName() const override { return "ThrowingObjectStorage"; }
    ObjectStorageType getType() const override { return ObjectStorageType::None; }
    std::string getCommonKeyPrefix() const override { return ""; }
    std::string getDescription() const override { return "test stub"; }
    String getObjectsNamespace() const override { return ""; }
    bool isRemote() const override { return true; }
    void startup() override { }
    void shutdown() override { }

    ObjectMetadata getObjectMetadata(const std::string &, bool) const override { unexpected("getObjectMetadata"); }
    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string &, bool) const override
    {
        unexpected("tryGetObjectMetadata");
    }
    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject &,
        const ReadSettings &,
        std::optional<size_t>,
        bool,
        bool) const override
    {
        unexpected("readObject");
    }
    void removeObjectIfExists(const StoredObject &) override { unexpected("removeObjectIfExists"); }
    void removeObjectsIfExist(
        const StoredObjects &,
        StoredObjects *) override { unexpected("removeObjectsIfExist"); }
    void copyObject( /// NOLINT
        const StoredObject &,
        const StoredObject &,
        const ReadSettings &,
        const WriteSettings &,
        std::optional<ObjectAttributes>) override
    {
        unexpected("copyObject");
    }
    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override { unexpected("createKeyGenerator"); }

private:
    [[noreturn]] static void unexpected(std::string_view method)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is not used by this test", method);
    }

    WriteErrorThrower throw_write_error;
    ExistsAnswer exists_answer;
    StorageOutcome storage_outcome;
    mutable size_t exists_calls = 0;
};

/// A storage whose failing buffer reports `outcome` and goes through `wrap`, as through a cache of the storage.
class WrappingObjectStorage final : public ThrowingObjectStorage
{
public:
    using Wrap = std::function<std::unique_ptr<WriteBufferFromFileBase>(std::unique_ptr<WriteBufferFromFileBase>)>;

    WrappingObjectStorage(std::optional<WriteOutcome> outcome_, Wrap wrap_)
        : ThrowingObjectStorage({}, [](size_t) { return false; }, {})
        , outcome(outcome_)
        , wrap(std::move(wrap_))
    {
    }

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode,
        std::optional<ObjectAttributes>,
        size_t,
        const WriteSettings &) override
    {
        return wrap(std::make_unique<FailingWriteBuffer>(
            [](const std::string & path) { throw Exception(ErrorCodes::S3_ERROR, "Write of {} failed in the test stub", path); },
            object.remote_path,
            outcome));
    }

private:
    std::optional<WriteOutcome> outcome;
    Wrap wrap;
};

/// Reports `outcome` and from then on fails every allocation of its thread, as at a memory limit, unless it is blocked.
class FaultingWriteBuffer final : public FailingWriteBuffer
{
public:
    FaultingWriteBuffer(std::string path_, WriteOutcome outcome_, MemoryTracker & tracker_)
        : FailingWriteBuffer(
              [](const std::string & path) { throw Exception(ErrorCodes::S3_ERROR, "Write of {} failed in the test stub", path); },
              std::move(path_),
              outcome_)
        , tracker(tracker_)
    {
    }

    std::optional<WriteOutcome> getWriteOutcome() const override
    {
        tracker.setFaultProbability(1.0);
        return FailingWriteBuffer::getWriteOutcome();
    }

private:
    MemoryTracker & tracker;
};

class FaultingObjectStorage final : public ThrowingObjectStorage
{
public:
    FaultingObjectStorage(WriteOutcome outcome_, MemoryTracker & tracker_)
        : ThrowingObjectStorage({}, [](size_t) { return false; }, {})
        , outcome(outcome_)
        , tracker(tracker_)
    {
    }

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode,
        std::optional<ObjectAttributes>,
        size_t,
        const WriteSettings &) override
    {
        return std::make_unique<FaultingWriteBuffer>(object.remote_path, outcome, tracker);
    }

private:
    WriteOutcome outcome;
    MemoryTracker & tracker;
};

/// Drives the real commit against `object_storage`.
bool commitTo(ObjectStoragePtr object_storage)
{
    Iceberg::IcebergPathResolver resolver(
        "/table",
        "/table",
        Iceberg::BlobStorageDescription{.type_name = "local", .namespace_name = "", .allow_foreign_namespaces = false});
    GeneratedMetadataFileWithInfo metadata_file_info{
        .path = Iceberg::IcebergPathFromMetadata::deserialize("/table/metadata/v2.metadata.json"),
        .version = 2,
        .compression_method = CompressionMethod::None,
    };

    return Iceberg::writeMetadataFileAndVersionHint(
        resolver,
        metadata_file_info,
        "{}",
        Iceberg::IcebergPathFromMetadata::deserialize("/table/metadata/version-hint.text"),
        object_storage,
        getContext().context,
        /*try_write_version_hint=*/ false);
}

/// Drives the real commit against a stub whose write fails with whatever `throw_write_error` throws.
bool commitWithFailingWrite(
    WriteErrorThrower throw_write_error, ExistsAnswer exists_answer = [](size_t) { return false; }, StorageOutcome storage_outcome = {})
{
    return commitTo(std::make_shared<ThrowingObjectStorage>(std::move(throw_write_error), std::move(exists_answer), storage_outcome));
}

/// Same, with a write that fails with a plain `Exception` carrying `write_error_code`.
bool commitWithFailingWrite(int write_error_code)
{
    return commitWithFailingWrite([write_error_code](const std::string & path)
    {
        throw Exception(write_error_code, "Write of {} failed in the test stub", path);
    });
}

/// Expects the commit to report that its outcome is unknown.
void expectUnknownOutcome(
    WriteErrorThrower throw_write_error, ExistsAnswer exists_answer = [](size_t) { return false; }, StorageOutcome storage_outcome = {})
{
    try
    {
        bool committed = commitWithFailingWrite(std::move(throw_write_error), std::move(exists_answer), storage_outcome);
        FAIL() << "Expected an unknown outcome, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << e.message();
    }
}

void throwStorageError(const std::string & path)
{
    throw Exception(ErrorCodes::S3_ERROR, "Write of {} failed in the test stub", path);
}

/// Expects the error of the storage to propagate, so that the commit rolls back.
void expectStorageErrorToPropagate(StorageOutcome storage_outcome)
{
    try
    {
        bool committed = commitWithFailingWrite(throwStorageError, [](size_t) { return false; }, storage_outcome);
        FAIL() << "Expected the error of the storage to propagate, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::S3_ERROR) << e.message();
    }
}

}

TEST(IcebergCommitPropagation, TrackedWriteThatMayHaveBeenAppliedIsNotRetried)
{
    /// The storage could not prove whether an attempt of the upload was applied.
    expectUnknownOutcome(throwStorageError, [](size_t) { return false; }, WriteOutcome::Unknown);

    /// The tracked outcome wins over the error: the conflict may be the answer to a replay of this write.
    expectUnknownOutcome(
        [](const std::string & path) { throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "{} exists in the test stub", path); },
        [](size_t) { return false; },
        WriteOutcome::Unknown);
}

TEST(IcebergCommitPropagation, TrackedWriteThatWasNotAppliedPropagates)
{
    /// Every attempt of the upload got an answer that refuses it, so nothing was published.
    expectStorageErrorToPropagate(WriteOutcome::NotApplied);
}

TEST(IcebergCommitPropagation, TrackedLostRaceStaysRetryable)
{
    /// The storage proved that another writer created the file, so the commit retries on the next version. Without this
    /// the other tests would also pass against a commit that propagates everything.
    EXPECT_FALSE(commitWithFailingWrite(throwStorageError, [](size_t) { return false; }, WriteOutcome::ConditionFailed));
}

TEST(IcebergCommitPropagation, TrackedRefusalIsNotALostRace)
{
    /// The outcome decides, not the error: a conflict that the storage did not report as a failed condition rolls back.
    try
    {
        bool committed = commitWithFailingWrite(
            [](const std::string & path) { throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "{} exists in the test stub", path); },
            [](size_t) { return false; },
            WriteOutcome::NotApplied);
        FAIL() << "Expected the error of the storage to propagate, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::FILE_ALREADY_EXISTS) << e.message();
    }
}

TEST(IcebergCommitPropagation, LostRaceThroughADecoratorStaysRetryable)
{
    /// A cache wraps the buffer of the storage, e.g. in `CachedOnDiskWriteBufferFromFile`, and forwards its outcome.
    EXPECT_FALSE(commitTo(std::make_shared<WrappingObjectStorage>(
        WriteOutcome::ConditionFailed,
        [](std::unique_ptr<WriteBufferFromFileBase> buffer) -> std::unique_ptr<WriteBufferFromFileBase>
        { return std::make_unique<WriteBufferFromFileDecorator>(std::move(buffer)); })));
}

TEST(IcebergCommitPropagation, WriteWithoutAnOutcomeIsUnknown)
{
    /// Nothing tells whether the metadata file is published, so the commit keeps its files.
    try
    {
        bool committed = commitTo(std::make_shared<WrappingObjectStorage>(
            std::nullopt, [](std::unique_ptr<WriteBufferFromFileBase> buffer) { return buffer; }));
        FAIL() << "Expected an unknown outcome, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << e.message();
        EXPECT_TRUE(e.message().contains("the storage reports no outcome of the write")) << e.message();
    }
}

TEST(IcebergCommitPropagation, MemoryLimitInTheDiagnosticsKeepsTheOutcome)
{
    /// Every allocation may throw, as with a small `min_allocation_size_to_throw_on_memory_limit`.
    const UInt64 saved_min_allocation_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);
    SCOPE_EXIT(CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_allocation_to_throw));

    for (const auto outcome : {WriteOutcome::Applied, WriteOutcome::Unknown})
    {
        MemoryTracker user_tracker(&total_memory_tracker, VariableContext::User, false);
        std::optional<bool> committed;
        std::optional<int> error_code;
        std::thread([&]
        {
            ThreadStatus thread_status;
            thread_status.memory_tracker.setParent(&user_tracker);
            thread_status.untracked_memory_limit = 0;
            try
            {
                committed = commitTo(std::make_shared<FaultingObjectStorage>(outcome, user_tracker));
            }
            catch (const Exception & e)
            {
                user_tracker.setFaultProbability(0);
                error_code = e.code();
            }
            user_tracker.setFaultProbability(0);
        }).join();

        if (outcome == WriteOutcome::Applied)
        {
            EXPECT_EQ(committed, true) << "error code " << error_code.value_or(0);
        }
        else
        {
            EXPECT_EQ(error_code, ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << "error code " << error_code.value_or(0);
        }
    }
}

TEST(IcebergCommitPropagation, TrackedWriteThatWasAppliedIsPublished)
{
    /// The upload succeeded, and a later step of the write failed, e.g. the check after the upload.
    EXPECT_TRUE(commitWithFailingWrite(throwStorageError, [](size_t) { return false; }, WriteOutcome::Applied));
}

TEST(IcebergCommitPropagation, RefusedConditionalWriteIsNotReportedAsALostRace)
{
    /// The backend cannot perform the write at all (it cannot express the compare-and-swap, or the disk is
    /// read-only), so retrying can never succeed: the commit must surface the refusal rather than return `false`.
    for (int code : {ErrorCodes::UNSUPPORTED_METHOD, ErrorCodes::READONLY})
    {
        try
        {
            bool committed = commitWithFailingWrite(code);
            FAIL() << "Expected error " << code << " to propagate, got " << committed;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), code) << e.message();
        }
    }
}

TEST(IcebergCommitPropagation, FailedCheckBeforeTheWriteStaysRetryable)
{
    /// Nothing was sent, so nothing can be published.
    EXPECT_FALSE(commitWithFailingWrite(
        [](const std::string &) { throw Exception(ErrorCodes::LOGICAL_ERROR, "The write must not be attempted"); },
        [](size_t) -> bool { throw Exception(ErrorCodes::NETWORK_ERROR, "The existence check failed in the test stub"); }));
}

TEST(IcebergCommitPropagation, FailedCreationOfTheWriteIsNotApplied)
{
    /// The storage could not create the buffer of the write, so nothing was sent, and the commit rolls back.
    try
    {
        bool committed = commitWithFailingWrite(ErrorCodes::NETWORK_ERROR);
        FAIL() << "Expected the error of the storage to propagate, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NETWORK_ERROR) << e.message();
    }
}

TEST(IcebergCommitPropagation, PermissionErrorIsNotReportedAsALostRace)
{
    /// The writer may not write the metadata file (local storage), so every retry is denied again:
    /// the commit must surface the error with its errno rather than return `false`.
    for (int error : {EACCES, EPERM, EROFS})
    {
        try
        {
            bool committed = commitWithFailingWrite([error](const std::string & path)
            {
                ErrnoException::throwFromPathWithErrno(ErrorCodes::CANNOT_OPEN_FILE, path, error, "Cannot open file {}", path);
            });
            FAIL() << "Expected errno " << error << " to propagate, got " << committed;
        }
        catch (const ErrnoException & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::CANNOT_OPEN_FILE) << e.message();
            EXPECT_EQ(e.getErrno(), error) << e.message();
        }
    }
}

#if USE_AWS_S3
TEST(IcebergCommitPropagation, S3AccessDeniedIsNotReportedAsALostRace)
{
    /// S3 rejected the request's credentials or permissions, so every retry is rejected again: the
    /// commit must surface the S3 error rather than return `false`.
    for (auto error : {
             Aws::S3::S3Errors::ACCESS_DENIED,
             Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID,
             Aws::S3::S3Errors::INVALID_SIGNATURE,
             Aws::S3::S3Errors::SIGNATURE_DOES_NOT_MATCH})
    {
        try
        {
            bool committed = commitWithFailingWrite([error](const std::string &) { throw S3Exception("Access Denied.", error); });
            FAIL() << "Expected S3 error " << static_cast<int>(error) << " to propagate, got " << committed;
        }
        catch (const S3Exception & e)
        {
            EXPECT_EQ(e.getS3ErrorCode(), error) << e.message();
        }
    }
}

TEST(IcebergCommitPropagation, RefusedExistenceCheckIsNotReportedAsALostRace)
{
    /// S3 answers 403 to a HEAD of an absent key without `s3:ListBucket`. Nothing was written yet, and every retry is
    /// refused again, so the commit must surface the error without trying the write.
    try
    {
        bool committed = commitWithFailingWrite(
            [](const std::string &) { throw Exception(ErrorCodes::LOGICAL_ERROR, "The write must not be attempted"); },
            [](size_t) -> bool { throw S3Exception("Access Denied.", Aws::S3::S3Errors::ACCESS_DENIED); });
        FAIL() << "Expected the refused existence check to propagate, got " << committed;
    }
    catch (const S3Exception & e)
    {
        EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::ACCESS_DENIED) << e.message();
    }
}
#endif

#endif
