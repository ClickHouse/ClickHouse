#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <optional>
#include <vector>

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

/// Keeps what is written in `target` once the write is finalized. With `error_after_store` it then
/// throws, which is a response lost after the store accepted the object.
class StringWriter : public WriteBufferFromFileBase
{
public:
    explicit StringWriter(std::optional<std::string> & target_, std::optional<int> error_after_store_ = {})
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, /*existing_memory=*/nullptr, /*alignment=*/0)
        , target(target_)
        , error_after_store(error_after_store_)
    {
    }

    void sync() override { }
    std::string getFileName() const override { return "string_writer"; }

private:
    void nextImpl() override { data.append(working_buffer.begin(), pos); }

    void finalizeImpl() override
    {
        next();
        target = std::move(data);
        if (error_after_store)
            throw Exception(*error_after_store, "Response lost after the test stub stored the object");
    }

    std::optional<std::string> & target;
    std::optional<int> error_after_store;
    std::string data;
};

/// What the stub store holds and how it fails. The conditional metadata write fails with
/// `write_error_code`: before storing anything, or after storing the written bytes when
/// `accept_write` is set. The store then holds `stored_content` (`std::nullopt` for an absent
/// target), the first `hidden_probes` probes after the write still report it absent, and the
/// read-back fails when `read_throws` is set. That is exactly the input the commit has to
/// distinguish: the error alone cannot tell a lost race from a response lost after the store
/// accepted the object. `version-hint.text` holds `hint`, its first `hint_probe_failures` probes
/// throw, and its first `hint_conflicts` writes lose to another writer that advances it by one.
struct StoreState
{
    int write_error_code = ErrorCodes::NETWORK_ERROR;
    bool accept_write = false;
    std::optional<std::string> stored_content;
    size_t hidden_probes = 0;
    bool read_throws = false;
    std::optional<std::string> hint;
    size_t hint_probe_failures = 0;
    size_t hint_conflicts = 0;
};

class ReconcilingObjectStorage : public IObjectStorage
{
public:
    explicit ReconcilingObjectStorage(StoreState state_) : state(std::move(state_)) { }

    const std::optional<std::string> & hint() const { return state.hint; }

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode,
        std::optional<ObjectAttributes>,
        size_t,
        const WriteSettings & write_settings) override
    {
        if (isVersionHint(object))
        {
            if (state.hint_conflicts > 0)
            {
                --state.hint_conflicts;
                state.hint = std::to_string(parse<Int32>(state.hint.value_or("0")) + 1);
                throw Exception(ErrorCodes::NETWORK_ERROR, "Another writer advanced the version hint first");
            }
            return std::make_unique<StringWriter>(state.hint);
        }

        /// The commit must ask for the metadata file to be created exclusively: without the
        /// condition the write is a plain overwrite and one of two concurrent writers is lost.
        /// `EXPECT_*` keeps the throw below reachable.
        EXPECT_EQ(write_settings.object_storage_write_if_none_match, "*");
        EXPECT_TRUE(write_settings.object_storage_write_if_match.empty());

        ++writes;
        if (state.accept_write)
            return std::make_unique<StringWriter>(state.stored_content, state.write_error_code);
        throw Exception(state.write_error_code, "Write of {} failed in the test stub", object.remote_path);
    }

    /// Before the write the target is absent so the commit proceeds; afterwards it reports whatever
    /// the store holds.
    bool exists(const StoredObject & object) const override
    {
        if (isVersionHint(object))
        {
            if (state.hint_probe_failures > 0)
            {
                --state.hint_probe_failures;
                throw Exception(ErrorCodes::NETWORK_ERROR, "Version hint probe failed in the test stub");
            }
            return state.hint.has_value();
        }
        return writes > 0 && state.stored_content.has_value() && ++probes_after_write > state.hidden_probes;
    }

    ObjectMetadata getObjectMetadata(const std::string &, bool) const override
    {
        ObjectMetadata metadata;
        metadata.size_bytes = state.stored_content ? state.stored_content->size() : 0;
        return metadata;
    }

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings &,
        std::optional<size_t>,
        bool,
        bool) const override
    {
        if (isVersionHint(object))
            return std::make_unique<StringReader>(state.hint.value_or(""));
        if (state.read_throws)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Read failed in the test stub");
        return std::make_unique<StringReader>(state.stored_content.value_or(""));
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

    static bool isVersionHint(const StoredObject & object) { return object.remote_path.ends_with("version-hint.text"); }

    mutable StoreState state;
    mutable size_t writes = 0;
    mutable size_t probes_after_write = 0;
};

constexpr auto committed_content = "{\"format-version\":2}";

bool commit(
    const std::shared_ptr<ReconcilingObjectStorage> & storage,
    CompressionMethod compression_method = CompressionMethod::None,
    Int32 version = 2)
{
    Iceberg::IcebergPathResolver resolver(
        "/table",
        "/table",
        Iceberg::BlobStorageDescription{.type_name = "local", .namespace_name = "", .allow_foreign_namespaces = false});
    GeneratedMetadataFileWithInfo metadata_file_info{
        .path = Iceberg::IcebergPathFromMetadata::deserialize(fmt::format("/table/metadata/v{}.metadata.json", version)),
        .version = version,
        .compression_method = compression_method,
    };

    return Iceberg::writeMetadataFileAndVersionHint(
        resolver,
        metadata_file_info,
        committed_content,
        Iceberg::IcebergPathFromMetadata::deserialize("/table/metadata/version-hint.text"),
        storage,
        getContext().context,
        /*try_write_version_hint=*/ false);
}

bool commitAgainst(StoreState state, CompressionMethod compression_method = CompressionMethod::None)
{
    return commit(std::make_shared<ReconcilingObjectStorage>(std::move(state)), compression_method);
}

}

TEST(IcebergCommitPropagation, RefusedConditionalWriteIsNotReportedAsALostRace)
{
    /// The backend refused the compare-and-swap, so retrying can never succeed: the commit must
    /// surface the refusal rather than return `false`, which callers read as a lost race.
    try
    {
        StoreState state;
        state.write_error_code = ErrorCodes::UNSUPPORTED_METHOD;
        bool committed = commitAgainst(state);
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
    StoreState state;
    state.stored_content = committed_content;
    EXPECT_TRUE(commitAgainst(state));
}

TEST(IcebergCommitPropagation, CompressedDocumentOfThisWriterIsCommitted)
{
    /// The read-back must decode what the write path encoded, or this writer's own document reads as
    /// another writer's and licenses the cleanup of a commit that took effect.
    std::vector<CompressionMethod> methods{CompressionMethod::Gzip, CompressionMethod::Zstd};
#if USE_SNAPPY
    methods.push_back(CompressionMethod::Snappy);
#endif
    StoreState state;
    state.accept_write = true;
    for (auto method : methods)
        EXPECT_TRUE(commitAgainst(state, method)) << toContentEncodingName(method);
}

TEST(IcebergCommitPropagation, TargetBecomingVisibleAfterTheErrorIsCommitted)
{
    /// The first read-back still finds nothing, which a single probe would misreport as unknown.
    StoreState state;
    state.stored_content = committed_content;
    state.hidden_probes = 1;
    EXPECT_TRUE(commitAgainst(state));
}

TEST(IcebergCommitPropagation, VersionHintAdvancesPastATransientProbeFailure)
{
    /// The commit has taken effect before the hint is touched, so a failure there must neither reach the
    /// caller, whose cleanup would delete the files of the snapshot that is now current, nor stop the hint
    /// from advancing.
    StoreState state;
    state.stored_content = committed_content;
    state.hint = "1";
    state.hint_probe_failures = 1;
    auto storage = std::make_shared<ReconcilingObjectStorage>(state);
    EXPECT_TRUE(commit(storage));
    EXPECT_EQ(storage->hint().value_or("<absent>"), "2");
}

TEST(IcebergCommitPropagation, VersionHintConvergesPastConcurrentWriters)
{
    /// Every lost race finds the hint advanced by another writer, which is progress, so it must not use up
    /// the attempts of the writer that committed the highest version.
    StoreState state;
    state.stored_content = committed_content;
    state.hint = "1";
    state.hint_conflicts = 4;
    auto storage = std::make_shared<ReconcilingObjectStorage>(state);
    EXPECT_TRUE(commit(storage, CompressionMethod::None, /*version=*/ 6));
    EXPECT_EQ(storage->hint().value_or("<absent>"), "6");
}

TEST(IcebergCommitPropagation, ContentOfAnotherWriterIsALostRace)
{
    /// Someone else's document occupies the version, which is the only outcome that proves this
    /// commit did not happen, so the staged files are garbage and cleanup is correct.
    StoreState state;
    state.stored_content = R"({"format-version":2,"other":true})";
    EXPECT_FALSE(commitAgainst(state));
}

TEST(IcebergCommitPropagation, AbsentTargetIsUnknownRatherThanALostRace)
{
    /// Nothing cancels a failed conditional write server-side and there is no ordering fence, so an
    /// absent target does not prove the write will not land. Treating it as a lost race deletes the
    /// files of a commit that then becomes visible.
    try
    {
        bool committed = commitAgainst(StoreState{});
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
        StoreState state;
        state.stored_content = committed_content;
        state.read_throws = true;
        bool committed = commitAgainst(state);
        FAIL() << "Expected an unknown commit state, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << e.message();
    }
}

#endif
