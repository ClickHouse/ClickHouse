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
}

namespace
{

using WriteErrorThrower = std::function<void(const std::string & path)>;
using ExistsAnswer = std::function<bool(size_t call)>;

/// `writeMetadataFileAndVersionHint` reports a failed commit as `false`, which every caller reads as
/// a lost compare-and-swap and retries. A write the storage refused (it cannot express the condition,
/// or it denies the write) fails the same way on every retry, so it has to propagate instead of being
/// reported as a lost race; everything else stays retryable. This stub makes both outcomes observable
/// without an object storage: `exists` answers what the test supplies (absent by default, so the
/// function proceeds to the write), and `writeObject` throws the error the test supplies. Everything
/// else throws, so an unexpected call is loud rather than silently absorbed.
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

TEST(IcebergCommitPropagation, OtherWriteFailuresStayRetryable)
{
    /// Any other failure may succeed on a retry, including a genuinely lost race, so it keeps being
    /// reported as an unsuccessful commit. Without this the other tests would also pass against a
    /// commit that propagates everything, which would make every transient error fatal.
    EXPECT_FALSE(commitWithFailingWrite(ErrorCodes::NETWORK_ERROR));

    /// A lost race on local storage.
    EXPECT_FALSE(commitWithFailingWrite(ErrorCodes::FILE_ALREADY_EXISTS));

    /// An I/O error, which may be transient.
    EXPECT_FALSE(commitWithFailingWrite([](const std::string & path)
    {
        ErrnoException::throwFromPathWithErrno(ErrorCodes::CANNOT_OPEN_FILE, path, EIO, "Cannot open file {}", path);
    }));

#if USE_AWS_S3
    /// A lost race on S3 (412 `PreconditionFailed`), which the SDK reports without a typed error code.
    EXPECT_FALSE(commitWithFailingWrite([](const std::string &)
    {
        throw S3Exception("At least one of the pre-conditions you specified did not hold", Aws::S3::S3Errors::UNKNOWN);
    }));
#endif
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

TEST(IcebergCommitPropagation, RefusalOfAWriteThatMayHaveLandedStaysRetryable)
{
    /// A refusal after the write was sent (e.g. of the check after an upload) is not proof that nothing was written.
    auto refuse = [](const std::string & path)
    {
        ErrnoException::throwFromPathWithErrno(ErrorCodes::CANNOT_OPEN_FILE, path, EACCES, "Cannot open file {}", path);
    };

    /// The metadata file is there after the refusal.
    EXPECT_FALSE(commitWithFailingWrite(refuse, [](size_t call) { return call > 0; }));

    /// The check after the refusal fails, so nothing is known.
    EXPECT_FALSE(commitWithFailingWrite(refuse, [](size_t call) -> bool
    {
        if (call == 0)
            return false;
        throw Exception(ErrorCodes::NETWORK_ERROR, "The existence check failed in the test stub");
    }));
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
