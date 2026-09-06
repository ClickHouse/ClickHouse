#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <optional>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
}

namespace
{

/// Serves a fixed body, so the commit's read-back sees a chosen document. Copies into the buffer the
/// caller owns rather than pointing at its own storage, which is what the read pipeline requires
/// when it supplies external memory.
class StringReader : public ReadBufferFromFileBase
{
public:
    explicit StringReader(std::string data_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, /*existing_memory=*/nullptr, /*alignment=*/0, data_.size())
        , data(std::move(data_))
    {
    }

    bool nextImpl() override
    {
        if (file_pos >= data.size() || internal_buffer.empty())
            return false;
        const size_t n = std::min(internal_buffer.size(), data.size() - file_pos);
        memcpy(internal_buffer.begin(), data.data() + file_pos, n);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        pos = working_buffer.begin();
        file_pos += n;
        return n != 0;
    }

    off_t seek(off_t off, int) override
    {
        file_pos = static_cast<size_t>(off);
        resetWorkingBuffer();
        return off;
    }
    off_t getPosition() override { return static_cast<off_t>(file_pos) - static_cast<off_t>(available()); }
    String getFileName() const override { return "string_reader"; }
    bool supportsExternalBufferMode() const override { return true; }

private:
    std::string data;
    size_t file_pos = 0;
};

/// Drives the real commit without an object storage. The conditional write always fails with
/// `write_error_code`; what the store is then found to hold is `stored_content` (`std::nullopt` for
/// an absent target), or the read-back itself fails when `read_throws` is set. That is exactly the
/// input the commit has to distinguish: the error alone cannot tell a lost race from a response lost
/// after the store accepted the object.
class ReconcilingObjectStorage : public IObjectStorage
{
public:
    ReconcilingObjectStorage(int write_error_code_, std::optional<std::string> stored_content_, bool read_throws_ = false)
        : write_error_code(write_error_code_), stored_content(std::move(stored_content_)), read_throws(read_throws_)
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
        /// condition the write is a plain overwrite and one of two concurrent writers is lost.
        /// `EXPECT_*` keeps the throw below reachable.
        EXPECT_EQ(write_settings.object_storage_write_if_none_match, "*");
        EXPECT_TRUE(write_settings.object_storage_write_if_match.empty());

        ++writes;
        throw Exception(write_error_code, "Write of {} failed in the test stub", object.remote_path);
    }

    /// Before the write the target is absent so the commit proceeds; afterwards it reports whatever
    /// the store was configured to hold.
    bool exists(const StoredObject &) const override { return writes > 0 && stored_content.has_value(); }

    ObjectMetadata getObjectMetadata(const std::string &, bool) const override
    {
        ObjectMetadata metadata;
        metadata.size_bytes = stored_content ? stored_content->size() : 0;
        return metadata;
    }

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject &,
        const ReadSettings &,
        std::optional<size_t>,
        bool,
        bool) const override
    {
        if (read_throws)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Read failed in the test stub");
        return std::make_unique<StringReader>(stored_content.value_or(""));
    }

    std::string getName() const override { return "ReconcilingObjectStorage"; }
    ObjectStorageType getType() const override { return ObjectStorageType::None; }
    std::string getCommonKeyPrefix() const override { return ""; }
    std::string getDescription() const override { return "test stub"; }
    String getObjectsNamespace() const override { return ""; }
    bool isRemote() const override { return true; }
    void startup() override { }
    void shutdown() override { }

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string &, bool) const override
    {
        unexpected("tryGetObjectMetadata");
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
    std::optional<std::string> stored_content;
    bool read_throws;
    mutable size_t writes = 0;
};

constexpr auto committed_content = "{\"format-version\":2}";

bool commitAgainst(int write_error_code, std::optional<std::string> stored_content, bool read_throws = false)
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
        committed_content,
        Iceberg::IcebergPathFromMetadata::deserialize("/table/metadata/version-hint.text"),
        std::make_shared<ReconcilingObjectStorage>(write_error_code, std::move(stored_content), read_throws),
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
        bool committed = commitAgainst(ErrorCodes::UNSUPPORTED_METHOD, std::nullopt);
        FAIL() << "Expected the refusal to propagate, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD) << e.message();
    }
}

TEST(IcebergCommitPropagation, StoredContentOfThisWriterIsCommitted)
{
    /// The response was lost after the store accepted the object, so the commit did take effect and
    /// the files it staged are the ones the table now points at. Reporting a lost race here is what
    /// makes callers delete them.
    EXPECT_TRUE(commitAgainst(ErrorCodes::NETWORK_ERROR, committed_content));
}

TEST(IcebergCommitPropagation, ContentOfAnotherWriterIsALostRace)
{
    /// Someone else's document occupies the version, which is the only outcome that proves this
    /// commit did not happen, so the staged files are garbage and cleanup is correct.
    EXPECT_FALSE(commitAgainst(ErrorCodes::NETWORK_ERROR, "{\"format-version\":2,\"other\":true}"));
}

TEST(IcebergCommitPropagation, AbsentTargetIsUnknownRatherThanALostRace)
{
    /// Nothing cancels a failed conditional write server-side and there is no ordering fence, so an
    /// absent target does not prove the write will not land. Treating it as a lost race deletes the
    /// files of a commit that then becomes visible.
    try
    {
        bool committed = commitAgainst(ErrorCodes::NETWORK_ERROR, std::nullopt);
        FAIL() << "Expected an unknown commit state, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << e.message();
        EXPECT_TRUE(Iceberg::isCommitStateUnknown(e));
    }
}

TEST(IcebergCommitPropagation, FailingReadBackIsUnknownRatherThanALostRace)
{
    /// The outcome could not be established at all, which is not the same claim as the commit not
    /// having happened.
    try
    {
        bool committed = commitAgainst(ErrorCodes::NETWORK_ERROR, committed_content, /*read_throws=*/ true);
        FAIL() << "Expected an unknown commit state, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << e.message();
    }
}

#endif
