#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Core/Defines.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromFileBase.h>
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
};

/// Drives the real commit against a stub whose write fails with `write_error_code`.
bool commitWithFailingWrite(int write_error_code)
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
        std::make_shared<ThrowingObjectStorage>(write_error_code),
        getContext().context,
        /*try_write_version_hint=*/ false);
}

/// Accepts and discards everything written to it: the commit only needs its writes to succeed.
class DiscardingWriteBuffer : public WriteBufferFromFileBase
{
public:
    explicit DiscardingWriteBuffer(std::string file_name_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), file_name(std::move(file_name_))
    {
    }

    void sync() override { }
    std::string getFileName() const override { return file_name; }

private:
    void nextImpl() override { }

    std::string file_name;
};

/// A backend that already has a `version-hint.text` and answers the read of it with the `ETag` it is
/// configured with - in particular with none at all, which is what an Azure-compatible endpoint that
/// omits the optional header produces. Records every write so the test can tell whether the hint was
/// rewritten and under which precondition.
class ExistingVersionHintObjectStorage : public IObjectStorage
{
public:
    struct Write
    {
        std::string path;
        std::string write_if_none_match;
        std::string write_if_match;
    };

    /// How another writer's `version-hint.text` interleaves with this commit. With `None` the hint is
    /// there from the start, with `Absent` it never is. With `CreatedBeforeTheExclusiveCreate` it is absent when the commit
    /// looks for it, and the exclusive write that would create it loses the race to another writer -
    /// which is exactly how a hint appearing in the middle of this commit looks from here. With
    /// `AppearsAfterTheMetadataFileIsWritten` it is absent when the commit looks for it and is there
    /// from the moment the metadata file is published: the commit never asks for it to be created,
    /// so nothing fails - only a re-read can notice it.
    enum class Race
    {
        None,
        Absent,
        CreatedBeforeTheExclusiveCreate,
        AppearsAfterTheMetadataFileIsWritten,
    };

    explicit ExistingVersionHintObjectStorage(std::string hint_etag_, Race race_ = Race::None)
        : hint_etag(std::move(hint_etag_)), race(race_), hint_present(race == Race::None)
    {
    }

    std::vector<Write> writes;
    std::vector<std::string> removed;

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode,
        std::optional<ObjectAttributes>,
        size_t,
        const WriteSettings & write_settings) override
    {
        writes.push_back(Write{
            object.remote_path,
            write_settings.object_storage_write_if_none_match,
            write_settings.object_storage_write_if_match});

        if (object.remote_path.ends_with(version_hint_name) && !hint_present)
        {
            /// The other writer got there first: the exclusive create fails and from now on the
            /// hint is there, with whatever tag this backend reports for it.
            hint_present = true;
            throw Exception(ErrorCodes::NETWORK_ERROR, "Another writer created {} first", object.remote_path);
        }

        if (object.remote_path.ends_with(metadata_file_name) && race == Race::AppearsAfterTheMetadataFileIsWritten)
            hint_present = true;

        return std::make_unique<DiscardingWriteBuffer>(object.remote_path);
    }

    /// The metadata file of the commit is new; the version hint is already there.
    bool exists(const StoredObject & object) const override
    {
        return object.remote_path.ends_with(version_hint_name) && hint_present;
    }

    SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata( /// NOLINT
        const StoredObject & object,
        const ReadSettings &,
        size_t,
        std::optional<size_t>) const override
    {
        EXPECT_TRUE(object.remote_path.ends_with(version_hint_name)) << object.remote_path;

        SmallObjectDataWithMetadata result;
        result.data = "1";
        result.metadata.etag = hint_etag;
        return result;
    }

    std::string getName() const override { return "ExistingVersionHintObjectStorage"; }
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
    void removeObjectIfExists(const StoredObject & object) override { removed.push_back(object.remote_path); }
    void removeObjectsIfExist(const StoredObjects &, StoredObjects *) override { unexpected("removeObjectsIfExist"); }
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

    static constexpr std::string_view version_hint_name = "version-hint.text";
    static constexpr std::string_view metadata_file_name = ".metadata.json";

    std::string hint_etag;
    Race race;
    bool hint_present;
};

/// Drives the real commit against a backend whose existing version hint carries `hint_etag`. The
/// outcome is left to the test, because a backend that reports no tag makes the commit throw, and
/// the writes recorded up to that point are what the test is about.
struct CommitOverExistingVersionHint
{
    using Race = ExistingVersionHintObjectStorage::Race;

    explicit CommitOverExistingVersionHint(const std::string & hint_etag, Race race = Race::None)
        : object_storage(std::make_shared<ExistingVersionHintObjectStorage>(hint_etag, race))
    {
    }

    bool run(bool try_write_version_hint = true) const
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
            try_write_version_hint);
    }

    const std::vector<ExistingVersionHintObjectStorage::Write> & writes() const { return object_storage->writes; }
    const std::vector<std::string> & removed() const { return object_storage->removed; }

    std::shared_ptr<ExistingVersionHintObjectStorage> object_storage;
};

}

TEST(IcebergCommitPropagation, VersionHintIsRewrittenUnderItsETag)
{
    /// The control: with an `ETag` the hint is advanced, and the rewrite carries the tag of the copy
    /// that was read as its compare-and-swap. Without this the test below would also pass against a
    /// commit that never touches the hint at all.
    CommitOverExistingVersionHint commit("\"abc\"");
    EXPECT_TRUE(commit.run());

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_EQ(writes[1].write_if_match, "\"abc\"");
    EXPECT_TRUE(writes[1].write_if_none_match.empty());
}

TEST(IcebergCommitPropagation, VersionHintWithoutETagFailsTheCommitBeforeAnythingIsPublished)
{
    /// `ETag` is an optional response header. Without it there is no compare-and-swap to put on the
    /// rewrite, so the update would degrade into an unconditional overwrite and two concurrent
    /// writers could move the hint backwards. Skipping the rewrite is no better: the commit would
    /// report success while every reader with `iceberg_use_version_hint = 1` keeps resolving the
    /// previous snapshot, because the hint is trusted without cross-checking the listing. The commit
    /// has to fail instead - and it has to fail before it writes anything, because `expireSnapshots`
    /// and the schema-alter path in `Mutations.cpp` do not remove the new metadata file when the
    /// commit throws, and a published file that no command committed is picked up by every reader
    /// that resolves the table by listing.
    CommitOverExistingVersionHint commit("");

    try
    {
        bool committed = commit.run();
        FAIL() << "Expected the commit to refuse to advance the version hint, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD) << e.message();
    }

    EXPECT_TRUE(commit.writes().empty()) << commit.writes().size() << " objects were written";
}

TEST(IcebergCommitPropagation, VersionHintAppearingMidCommitTakesTheMetadataFileBack)
{
    /// The hint is absent when the commit checks for it, and by the time the commit tries to create
    /// it another writer has, so the exclusive create loses and the retry reads a hint that carries
    /// no tag. The metadata file is published by then, so the commit removes it again before
    /// refusing - otherwise a snapshot no command committed would stay visible to listing-based
    /// readers.
    CommitOverExistingVersionHint commit("", CommitOverExistingVersionHint::Race::CreatedBeforeTheExclusiveCreate);

    try
    {
        bool committed = commit.run();
        FAIL() << "Expected the commit to refuse to advance the version hint, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD) << e.message();
    }

    /// The metadata file, then the exclusive create of the hint that lost the race.
    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[0].path.ends_with("v2.metadata.json")) << writes[0].path;
    EXPECT_EQ(writes[0].write_if_none_match, "*");
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_EQ(writes[1].write_if_none_match, "*");

    const auto & removed = commit.removed();
    ASSERT_EQ(removed.size(), 1u);
    EXPECT_EQ(removed[0], writes[0].path);
}

TEST(IcebergCommitPropagation, HintCreatedAfterThePreCheckIsStillAdvancedByAWriterThatDoesNotCreateHints)
{
    /// `try_write_version_hint = false`: this writer does not create the hint, but once any writer
    /// has, every commit must keep it in sync. Here the hint is absent when the commit checks it
    /// before publishing the metadata file, and another writer creates it - pointing at the previous
    /// version - while the file is being written. The pre-check saw nothing, so its result must not
    /// stand in for the read after the publish: the commit has to look again and advance the hint
    /// under the tag it reads, exactly as it did before the pre-check existed. Otherwise it returns
    /// success while readers with `iceberg_use_version_hint = 1` stay on the previous snapshot.
    CommitOverExistingVersionHint commit("\"abc\"", CommitOverExistingVersionHint::Race::AppearsAfterTheMetadataFileIsWritten);
    EXPECT_TRUE(commit.run(/*try_write_version_hint=*/ false));

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[0].path.ends_with("v2.metadata.json")) << writes[0].path;
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_EQ(writes[1].write_if_match, "\"abc\"");
    EXPECT_TRUE(writes[1].write_if_none_match.empty());
    EXPECT_TRUE(commit.removed().empty());
}

TEST(IcebergCommitPropagation, WriterThatDoesNotCreateHintsLeavesAnAbsentHintAlone)
{
    /// The control for the test above: when no hint appears, a writer with
    /// `try_write_version_hint = false` publishes the metadata file and nothing else. Without this
    /// the test above would also pass against a commit that creates the hint regardless of the flag.
    CommitOverExistingVersionHint commit("\"abc\"", CommitOverExistingVersionHint::Race::Absent);
    EXPECT_TRUE(commit.run(/*try_write_version_hint=*/ false));

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 1u);
    EXPECT_TRUE(writes[0].path.ends_with("v2.metadata.json")) << writes[0].path;
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
