#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <map>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

/// `writeMetadataFileAndVersionHint` reports a failed commit as `false`, which every caller reads as
/// a lost compare-and-swap and retries. A backend that cannot express the condition will never
/// succeed, so that refusal has to propagate instead of being reported as a lost race. This stub
/// makes both outcomes observable without an object storage: the metadata file is absent at the
/// existence check, so the function proceeds to the write, and `writeObject` fails with a
/// configurable error code. When `content_left_by_failed_write` is set, the failed write first
/// stores it under the target, as when another writer creates the file between the check and the
/// write; `exists` and `readObject` serve what is stored.
/// Everything else throws, so an unexpected call is loud rather than silently absorbed.
class ThrowingObjectStorage : public IObjectStorage
{
public:
    explicit ThrowingObjectStorage(int write_error_code_, std::optional<std::string> content_left_by_failed_write_ = {})
        : write_error_code(write_error_code_), content_left_by_failed_write(std::move(content_left_by_failed_write_))
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

        if (content_left_by_failed_write)
            objects[object.remote_path] = *content_left_by_failed_write;
        throw Exception(write_error_code, "Write of {} failed in the test stub", object.remote_path);
    }

    bool exists(const StoredObject & object) const override { return objects.contains(object.remote_path); }

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
        const StoredObject & object,
        const ReadSettings &,
        std::optional<size_t>,
        bool,
        bool) const override
    {
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "{} does not exist in the test stub", object.remote_path);
        return std::make_unique<ReadBufferFromOwnMemoryFile>(object.remote_path, it->second);
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

    int write_error_code;
    std::optional<std::string> content_left_by_failed_write;
    std::map<std::string, std::string> objects;
};

constexpr std::string_view committed_content = "{}";

/// Drives the real `commit` against `object_storage`, whose write of the metadata file fails.
template <typename Commit>
auto commitWithFailingWrite(Commit && commit, ObjectStoragePtr object_storage)
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

    return commit(
        resolver,
        metadata_file_info,
        std::string(committed_content),
        Iceberg::IcebergPathFromMetadata::deserialize("/table/metadata/version-hint.text"),
        object_storage,
        getContext().context,
        /*try_write_version_hint=*/ false);
}

bool commitWithFailingWrite(int write_error_code)
{
    return commitWithFailingWrite(Iceberg::writeMetadataFileAndVersionHint, std::make_shared<ThrowingObjectStorage>(write_error_code));
}

/// The conditional write is rejected the way `LocalObjectStorage` rejects a lost `link`, after the
/// target was created with `content_left_by_failed_write`.
Iceberg::MetadataWriteOutcome commitWithRejectedWrite(std::string content_left_by_failed_write)
{
    return commitWithFailingWrite(
        Iceberg::writeMetadataFileAndVersionHintWithOutcome,
        std::make_shared<ThrowingObjectStorage>(ErrorCodes::FILE_ALREADY_EXISTS, std::move(content_left_by_failed_write)));
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

TEST(IcebergCommitPropagation, LostRaceAfterTheExistenceCheckIsTargetExisted)
{
    /// Both writers passed the existence check and the other one created the file first. The name
    /// now holds its content, so nothing of this commit can ever be published there.
    EXPECT_EQ(commitWithRejectedWrite(R"({"current-snapshot-id": 2})"), Iceberg::MetadataWriteOutcome::TargetExisted);
}

TEST(IcebergCommitPropagation, OwnBytesAfterAFailedWriteAreNotALostRace)
{
    /// The file holds this commit's own content, as after a replay of a write that landed, so the
    /// commit may be published and its files must not be removed as those of a lost race.
    EXPECT_EQ(commitWithRejectedWrite(std::string(committed_content)), Iceberg::MetadataWriteOutcome::WriteFailed);
}

#endif
