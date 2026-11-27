#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// `writeMetadataFileAndVersionHint` reports a failed commit as `false`, which every caller reads as
/// a lost compare-and-swap and retries. A backend that cannot express the condition will never
/// succeed, so that refusal has to propagate instead of being reported as a lost race. This stub
/// makes both outcomes observable without an object storage: `exists` says the target is absent so
/// the function proceeds to the write, and `writeObject` fails with a configurable error code.
/// Everything else throws, so an unexpected call is loud rather than silently absorbed.
class ThrowingObjectStorage : public IObjectStorage
{
public:
    explicit ThrowingObjectStorage(int write_error_code_) : write_error_code(write_error_code_) { }

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode,
        std::optional<ObjectAttributes>,
        size_t,
        const WriteSettings & write_settings) override
    {
        /// The commit must ask for the metadata file to be created exclusively: without the
        /// condition the write is a plain overwrite and one of two concurrent writers is lost. Only
        /// the metadata write reaches this stub, because both tests leave the commit before the
        /// version-hint write. `EXPECT_*` keeps the throw below reachable.
        EXPECT_EQ(write_settings.object_storage_write_if_none_match, "*");
        EXPECT_TRUE(write_settings.object_storage_write_if_match.empty());

        throw Exception(write_error_code, "Write of {} failed in the test stub", object.remote_path);
    }

    /// The commit's existence probe: the metadata file is absent, so the write is attempted.
    bool exists(const StoredObject &) const override { return false; }

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
    void removeObjectsIfExist(const StoredObjects &) override { unexpected("removeObjectsIfExist"); }
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

    int write_error_code;
};

/// Drives the real commit against a stub whose write fails with `write_error_code`.
bool commitWithFailingWrite(int write_error_code)
{
    Iceberg::IcebergPathResolver resolver("/table", "/table");
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
        std::make_shared<ThrowingObjectStorage>(write_error_code),
        getContext().context,
        /*try_write_version_hint=*/ false);
}

}

TEST(IcebergCommitPropagation, RefusedConditionalWriteIsNotReportedAsALostRace)
{
    /// The backend refused the compare-and-swap, so retrying can never succeed: the commit must
    /// surface the refusal rather than return `false`, which callers read as a lost race.
    try
    {
        bool committed = commitWithFailingWrite(ErrorCodes::UNSUPPORTED_METHOD);
        FAIL() << "Expected the refusal to propagate, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD) << e.message();
    }
}

TEST(IcebergCommitPropagation, OtherWriteFailuresStayRetryable)
{
    /// Any other failure may succeed on a retry, including a genuinely lost race, so it keeps being
    /// reported as an unsuccessful commit. Without this the test above would also pass against a
    /// commit that propagates everything, which would make every transient error fatal.
    EXPECT_FALSE(commitWithFailingWrite(ErrorCodes::NETWORK_ERROR));
}

#endif
