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
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <optional>
#include <vector>

#if USE_AWS_S3
#include <IO/S3Common.h>
#endif

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_STATUS_OF_TRANSACTION;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int READONLY;
}

namespace
{

using WriteErrorThrower = std::function<void(const std::string & path)>;

/// A write that fails with a plain `Exception` carrying `code`.
WriteErrorThrower throwCode(int code)
{
    return [code](const std::string & path) { throw Exception(code, "Write of {} failed in the test stub", path); };
}

/// A write that fails like a local file the writer cannot open, with `error` as its errno.
WriteErrorThrower throwErrno(int error)
{
    return [error](const std::string & path)
    {
        ErrnoException::throwFromPathWithErrno(ErrorCodes::CANNOT_OPEN_FILE, path, error, "Cannot open file {}", path);
    };
}

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

/// Keeps what is written in `target` once the write is finalized. With `throw_after_store` it then
/// throws, which is a response lost after the store accepted the object.
class StringWriter : public WriteBufferFromFileBase
{
public:
    explicit StringWriter(std::optional<std::string> & target_, WriteErrorThrower throw_after_store_ = {})
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, /*existing_memory=*/nullptr, /*alignment=*/0)
        , target(target_)
        , throw_after_store(std::move(throw_after_store_))
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
        if (throw_after_store)
            throw_after_store(getFileName());
    }

    std::optional<std::string> & target;
    WriteErrorThrower throw_after_store;
    std::string data;
};

/// What the stub store holds and how it fails. The conditional metadata write fails with
/// `throw_write_error`: before storing anything, or after storing the written bytes when
/// `accept_write` is set. The store then holds `stored_content` (`std::nullopt` for an absent
/// target), and the read-back fails when `read_throws` is set. That is exactly the input the commit has to
/// distinguish: the error alone cannot tell a lost race from a response lost after the store
/// accepted the object. `target_exists`, when set, answers the target's existence checks instead, by call
/// number (call 0 is the commit's pre-check). `version-hint.text` holds `hint`, its first `hint_probe_failures`
/// probes throw, and its first `hint_conflicts` writes lose to another writer that advances it by one.
struct StoreState
{
    WriteErrorThrower throw_write_error = throwCode(ErrorCodes::NETWORK_ERROR);
    bool accept_write = false;
    std::optional<std::string> stored_content;
    bool read_throws = false;
    std::function<bool(size_t call)> target_exists;
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
            /// Without the compare-and-swap a writer of a lower version can overwrite a newer hint.
            if (state.hint)
            {
                EXPECT_EQ(write_settings.object_storage_write_if_match, hintEtag());
                EXPECT_TRUE(write_settings.object_storage_write_if_none_match.empty());
            }
            else
            {
                EXPECT_EQ(write_settings.object_storage_write_if_none_match, "*");
                EXPECT_TRUE(write_settings.object_storage_write_if_match.empty());
            }

            ++hint_writes;
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
            return std::make_unique<StringWriter>(state.stored_content, state.throw_write_error);
        state.throw_write_error(object.remote_path);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The stub must throw");
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
        const size_t call = target_checks++;
        if (state.target_exists)
            return state.target_exists(call);
        return writes > 0 && state.stored_content.has_value();
    }

    ObjectMetadata getObjectMetadata(const std::string &, bool) const override
    {
        ObjectMetadata metadata;
        metadata.size_bytes = state.stored_content ? state.stored_content->size() : 0;
        return metadata;
    }

    SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        size_t max_size_bytes,
        std::optional<size_t> read_hint) const override
    {
        auto result = IObjectStorage::readSmallObjectAndGetObjectMetadata(object, read_settings, max_size_bytes, read_hint);
        if (isVersionHint(object))
            result.metadata.etag = hintEtag();
        return result;
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

    /// Changes with every write of the hint, like a real ETag.
    std::string hintEtag() const { return fmt::format("hint-etag-{}", hint_writes); }

    mutable StoreState state;
    mutable size_t writes = 0;
    mutable size_t target_checks = 0;
    size_t hint_writes = 0;
};

constexpr auto committed_content = "{\"format-version\":2}";
constexpr auto another_writers_content = R"({"format-version":2,"other":true})";

bool commit(
    const std::shared_ptr<ReconcilingObjectStorage> & storage,
    CompressionMethod compression_method = CompressionMethod::None,
    Int32 version = 2,
    bool try_write_version_hint = false)
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
        try_write_version_hint);
}

bool commitAgainst(StoreState state, CompressionMethod compression_method = CompressionMethod::None)
{
    return commit(std::make_shared<ReconcilingObjectStorage>(std::move(state)), compression_method);
}

void expectUnknownCommitState(StoreState state)
{
    try
    {
        bool committed = commitAgainst(std::move(state));
        FAIL() << "Expected an unknown commit state, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_TRANSACTION) << e.message();
        EXPECT_TRUE(Iceberg::isCommitStateUnknown(e));
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
            StoreState state;
            state.throw_write_error = throwCode(code);
            bool committed = commitAgainst(state);
            FAIL() << "Expected error " << code << " to propagate, got " << committed;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), code) << e.message();
        }
    }
}

TEST(IcebergCommitPropagation, OtherWriteFailuresAreReconciled)
{
    /// Any other failure may be a lost race or a lost response, so it is not surfaced as itself: the outcome is
    /// established from what the store holds. Without this the refusal tests would also pass against a commit that
    /// propagates everything.
    std::vector<WriteErrorThrower> failures{
        throwCode(ErrorCodes::NETWORK_ERROR),
        /// A lost race on local storage.
        throwCode(ErrorCodes::FILE_ALREADY_EXISTS),
        /// An I/O error, which may be transient.
        throwErrno(EIO),
    };
#if USE_AWS_S3
    /// A lost race on S3 (412 `PreconditionFailed`), which the SDK reports without a typed error code.
    failures.push_back([](const std::string &)
    {
        throw S3Exception("At least one of the pre-conditions you specified did not hold", Aws::S3::S3Errors::UNKNOWN);
    });
#endif

    for (const auto & failure : failures)
    {
        StoreState lost_race;
        lost_race.throw_write_error = failure;
        lost_race.stored_content = another_writers_content;
        EXPECT_FALSE(commitAgainst(lost_race));

        StoreState absent;
        absent.throw_write_error = failure;
        expectUnknownCommitState(absent);
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
            StoreState state;
            state.throw_write_error = throwErrno(error);
            bool committed = commitAgainst(state);
            FAIL() << "Expected errno " << error << " to propagate, got " << committed;
        }
        catch (const ErrnoException & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::CANNOT_OPEN_FILE) << e.message();
            EXPECT_EQ(e.getErrno(), error) << e.message();
        }
    }
}

TEST(IcebergCommitPropagation, RefusalOfAWriteThatMayHaveLandedIsReconciled)
{
    /// A refusal after the write was sent (e.g. of the check after an upload) is not proof that nothing was written.

    /// This writer's document is there after the refusal.
    StoreState committed;
    committed.throw_write_error = throwErrno(EACCES);
    committed.accept_write = true;
    EXPECT_TRUE(commitAgainst(committed));

    /// Another writer's document is there after the refusal.
    StoreState lost_race;
    lost_race.throw_write_error = throwErrno(EACCES);
    lost_race.stored_content = another_writers_content;
    EXPECT_FALSE(commitAgainst(lost_race));

    /// The check after the refusal fails, so nothing is known.
    StoreState unknown;
    unknown.throw_write_error = throwErrno(EACCES);
    unknown.target_exists = [](size_t call) -> bool
    {
        if (call == 0)
            return false;
        throw Exception(ErrorCodes::NETWORK_ERROR, "The existence check failed in the test stub");
    };
    expectUnknownCommitState(unknown);
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
            StoreState state;
            state.throw_write_error = [error](const std::string &) { throw S3Exception("Access Denied.", error); };
            bool committed = commitAgainst(state);
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
        StoreState state;
        state.throw_write_error = [](const std::string &) { throw Exception(ErrorCodes::LOGICAL_ERROR, "The write must not be attempted"); };
        state.target_exists = [](size_t) -> bool { throw S3Exception("Access Denied.", Aws::S3::S3Errors::ACCESS_DENIED); };
        bool committed = commitAgainst(state);
        FAIL() << "Expected the refused existence check to propagate, got " << committed;
    }
    catch (const S3Exception & e)
    {
        EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::ACCESS_DENIED) << e.message();
    }
}
#endif

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

TEST(IcebergCommitPropagation, VersionHintIsCreatedExclusively)
{
    /// Two writers creating the hint at once must not both succeed, or the one with the lower version can win.
    StoreState state;
    state.stored_content = committed_content;
    auto storage = std::make_shared<ReconcilingObjectStorage>(state);
    EXPECT_TRUE(commit(storage, CompressionMethod::None, /*version=*/ 2, /*try_write_version_hint=*/ true));
    EXPECT_EQ(storage->hint().value_or("<absent>"), "2");
}

TEST(IcebergCommitPropagation, ContentOfAnotherWriterIsALostRace)
{
    /// Someone else's document occupies the version, which is the only outcome that proves this
    /// commit did not happen, so the staged files are garbage and cleanup is correct.
    StoreState state;
    state.stored_content = another_writers_content;
    EXPECT_FALSE(commitAgainst(state));
}

TEST(IcebergCommitPropagation, AbsentTargetIsUnknownRatherThanALostRace)
{
    /// Nothing cancels a failed conditional write server-side and there is no ordering fence, so an
    /// absent target does not prove the write will not land. Treating it as a lost race deletes the
    /// files of a commit that then becomes visible.
    expectUnknownCommitState(StoreState{});
}

TEST(IcebergCommitPropagation, FailingReadBackIsUnknownRatherThanALostRace)
{
    /// The outcome could not be established at all, which is not the same claim as the commit not
    /// having happened.
    StoreState state;
    state.stored_content = committed_content;
    state.read_throws = true;
    expectUnknownCommitState(state);
}

#endif
