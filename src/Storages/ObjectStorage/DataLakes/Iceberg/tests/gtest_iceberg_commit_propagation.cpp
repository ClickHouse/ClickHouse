#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <cerrno>
#include <functional>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
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
}

namespace
{

using WriteErrorThrower = std::function<void(const std::string & path)>;
using ExistsAnswer = std::function<bool(size_t call)>;

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
    ThrowingObjectStorage(WriteErrorThrower throw_write_error_, ExistsAnswer exists_answer_)
        : throw_write_error(std::move(throw_write_error_)), exists_answer(std::move(exists_answer_))
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
    mutable size_t exists_calls = 0;
};

/// Drives the real commit against a stub whose write fails with whatever `throw_write_error` throws.
bool commitWithFailingWrite(WriteErrorThrower throw_write_error, ExistsAnswer exists_answer = [](size_t) { return false; })
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
        std::make_shared<ThrowingObjectStorage>(std::move(throw_write_error), std::move(exists_answer)),
        getContext().context,
        /*try_write_version_hint=*/ false);
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
void expectUnknownOutcome(WriteErrorThrower throw_write_error, ExistsAnswer exists_answer = [](size_t) { return false; })
{
    try
    {
        bool committed = commitWithFailingWrite(std::move(throw_write_error), std::move(exists_answer));
        FAIL() << "Expected an unknown outcome, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << e.message();
    }
}

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

TEST(IcebergCommitPropagation, LostRaceStaysRetryable)
{
    /// Another writer published the version first, so this commit published nothing and is retried on the next
    /// version. Without this the other tests would also pass against a commit that propagates everything.

    /// A lost race on local storage.
    EXPECT_FALSE(commitWithFailingWrite(ErrorCodes::FILE_ALREADY_EXISTS));

#if USE_AWS_S3
    /// A lost race on S3 (412 `PreconditionFailed`). The SDK has no typed error code for it, so it reports the name in
    /// the message.
    EXPECT_FALSE(commitWithFailingWrite([](const std::string &)
    {
        throw S3Exception(
            "Unable to parse ExceptionName: PreconditionFailed Message: At least one of the pre-conditions you specified did not hold",
            Aws::S3::S3Errors::UNKNOWN);
    }));
#endif
}

TEST(IcebergCommitPropagation, FailedCheckBeforeTheWriteStaysRetryable)
{
    /// Nothing was sent, so nothing can be published.
    EXPECT_FALSE(commitWithFailingWrite(
        [](const std::string &) { throw Exception(ErrorCodes::LOGICAL_ERROR, "The write must not be attempted"); },
        [](size_t) -> bool { throw Exception(ErrorCodes::NETWORK_ERROR, "The existence check failed in the test stub"); }));
}

TEST(IcebergCommitPropagation, FailedWriteLeavesTheOutcomeUnknown)
{
    /// The write was sent, and its response tells nothing: the storage may have stored the metadata file, which then
    /// references the files that a rollback removes.
    expectUnknownOutcome([](const std::string & path)
    {
        throw Exception(ErrorCodes::NETWORK_ERROR, "Write of {} failed in the test stub", path);
    });

    /// An I/O error.
    expectUnknownOutcome([](const std::string & path)
    {
        ErrnoException::throwFromPathWithErrno(ErrorCodes::CANNOT_OPEN_FILE, path, EIO, "Cannot open file {}", path);
    });
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

TEST(IcebergCommitPropagation, RefusalOfAWriteThatMayHaveLandedLeavesTheOutcomeUnknown)
{
    /// A refusal after the write was sent (e.g. of the check after an upload) is not proof that nothing was written.
    auto refuse = [](const std::string & path)
    {
        ErrnoException::throwFromPathWithErrno(ErrorCodes::CANNOT_OPEN_FILE, path, EACCES, "Cannot open file {}", path);
    };

    /// The metadata file is there after the refusal.
    expectUnknownOutcome(refuse, [](size_t call) { return call > 0; });

    /// The check after the refusal fails, so nothing is known.
    expectUnknownOutcome(refuse, [](size_t call) -> bool
    {
        if (call == 0)
            return false;
        throw Exception(ErrorCodes::NETWORK_ERROR, "The existence check failed in the test stub");
    });
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

TEST(IcebergCommitPropagation, S3WriteWithUnknownOutcomeIsNotRetried)
{
    /// The S3 client could not prove whether an attempt of the upload was applied, and it marked the error.
    expectUnknownOutcome([](const std::string &)
    {
        S3Exception exception("Connection reset by peer", Aws::S3::S3Errors::NETWORK_CONNECTION);
        exception.setConditionalWriteVerdict(S3::ConditionalWriteVerdict::Unknown);
        throw exception; /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
    });

    /// The mark wins over a lost race: the 412 may be the answer to a replay of this write.
    expectUnknownOutcome([](const std::string &)
    {
        S3Exception exception(
            "Unable to parse ExceptionName: PreconditionFailed Message: At least one of the pre-conditions you specified did not hold",
            Aws::S3::S3Errors::UNKNOWN);
        exception.setConditionalWriteVerdict(S3::ConditionalWriteVerdict::Unknown);
        throw exception; /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
    });
}

TEST(IcebergCommitPropagation, S3WriteThatWasNotAppliedPropagates)
{
    /// Every attempt of the upload got an answer that refuses it, so nothing was published: the commit fails with the
    /// error of S3, and its caller rolls back.
    try
    {
        bool committed = commitWithFailingWrite([](const std::string &)
        {
            S3Exception exception("Slow Down.", Aws::S3::S3Errors::SLOW_DOWN);
            exception.setConditionalWriteVerdict(S3::ConditionalWriteVerdict::NotApplied);
            throw exception; /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
        });
        FAIL() << "Expected the S3 error to propagate, got " << committed;
    }
    catch (const S3Exception & e)
    {
        EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::SLOW_DOWN) << e.message();
    }
}

TEST(IcebergCommitPropagation, S3ErrorWithoutVerdictLeavesTheOutcomeUnknown)
{
    /// An error that the client did not decide, e.g. of the check after the upload, proves nothing about the upload.
    expectUnknownOutcome([](const std::string & path)
    {
        throw S3Exception(Aws::S3::S3Errors::NO_SUCH_KEY, "Object {} suddenly disappeared", path);
    });
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
