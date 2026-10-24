#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <cerrno>
#include <functional>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Core/Defines.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromFileBase.h>
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
    extern const int UNKNOWN_STATUS_OF_INSERT;
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

    /// The version hint is absent, so the commit's check of it before publishing the metadata file passes.
    /// For the metadata file, call 0 is the commit's pre-check (absent, so the write is attempted), later
    /// calls the check after a refusal.
    bool exists(const StoredObject & object) const override
    {
        if (object.remote_path.ends_with("version-hint.text"))
            return false;
        return exists_answer(exists_calls++);
    }

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
    /// so nothing fails - only a re-read can notice it. With `CreatedByThisCommit` it is absent and
    /// nobody else creates it, so the exclusive create of this commit succeeds.
    enum class Race
    {
        None,
        Absent,
        CreatedByThisCommit,
        CreatedBeforeTheExclusiveCreate,
        AppearsAfterTheMetadataFileIsWritten,
    };

    /// `head_etag` is what a metadata-only request reports for the hint; by default it agrees with
    /// the read.
    explicit ExistingVersionHintObjectStorage(
        std::string hint_etag_, Race race_ = Race::None, std::optional<std::string> head_etag_ = std::nullopt)
        : hint_etag(std::move(hint_etag_))
        , head_etag(head_etag_.value_or(hint_etag))
        , race(race_)
        , hint_present(race == Race::None)
    {
    }

    /// How a removal fails, as a transient network error would: either before the request takes
    /// effect, or after that, when only its response is lost.
    enum class RemovalFailure
    {
        None,
        BeforeApplying,
        AfterApplying,
    };

    std::vector<Write> writes;
    std::vector<std::string> removed;
    /// Every read of the hint fails, as a transient network error would.
    bool read_fails = false;
    /// How every removal of the hint fails.
    RemovalFailure hint_removal_failure = RemovalFailure::None;
    /// With `Race::CreatedByThisCommit`: another writer advances the hint this commit has created to
    /// this content before the commit reads it back.
    std::optional<std::string> advanced_by_another_writer_to;

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

        if (object.remote_path.ends_with(version_hint_name) && race == Race::CreatedByThisCommit)
        {
            hint_present = true;
            /// The version `CommitOverExistingVersionHint::run` commits.
            hint_content = advanced_by_another_writer_to.value_or("2");
            return std::make_unique<DiscardingWriteBuffer>(object.remote_path);
        }

        if (object.remote_path.ends_with(version_hint_name) && !hint_present)
        {
            /// The other writer got there first: the exclusive create fails and from now on the
            /// hint is there, with whatever tag this backend reports for it.
            hint_present = true;
            throw Exception(ErrorCodes::NETWORK_ERROR, "Another writer created {} first", object.remote_path);
        }

        if (object.remote_path.ends_with(metadata_file_name))
        {
            metadata_present = true;
            if (race == Race::AppearsAfterTheMetadataFileIsWritten)
                hint_present = true;
        }

        return std::make_unique<DiscardingWriteBuffer>(object.remote_path);
    }

    /// The metadata file of the commit is there once it is written; the version hint as `race` says.
    bool exists(const StoredObject & object) const override
    {
        if (object.remote_path.ends_with(version_hint_name))
            return hint_present;
        return object.remote_path.ends_with(metadata_file_name) && metadata_present;
    }

    bool hintPresent() const { return hint_present; }
    bool metadataPresent() const { return metadata_present; }
    bool metadataRemovalAttempted() const { return metadata_removal_attempted; }

    SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata( /// NOLINT
        const StoredObject & object,
        const ReadSettings &,
        size_t,
        std::optional<size_t>) const override
    {
        EXPECT_TRUE(object.remote_path.ends_with(version_hint_name)) << object.remote_path;
        if (read_fails)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot read {}", object.remote_path);

        SmallObjectDataWithMetadata result;
        result.data = hint_content;
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

    ObjectMetadata getObjectMetadata(const std::string & path, bool) const override
    {
        EXPECT_TRUE(path.ends_with(version_hint_name)) << path;
        EXPECT_TRUE(hint_present) << path;

        ObjectMetadata metadata;
        metadata.etag = head_etag;
        return metadata;
    }
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
    void removeObjectIfExists(const StoredObject & object) override
    {
        const bool is_hint = object.remote_path.ends_with(version_hint_name);
        const auto failure = is_hint ? hint_removal_failure : RemovalFailure::None;
        if (!is_hint)
            metadata_removal_attempted = true;
        if (failure == RemovalFailure::BeforeApplying)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot remove {}", object.remote_path);

        (is_hint ? hint_present : metadata_present) = false;
        if (failure == RemovalFailure::AfterApplying)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Lost the response to the removal of {}", object.remote_path);
        removed.push_back(object.remote_path);
    }
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
    std::string head_etag;
    Race race;
    bool hint_present;
    bool metadata_present = false;
    bool metadata_removal_attempted = false;
    std::string hint_content = "1";
};

/// Drives the real commit against a backend whose existing version hint carries `hint_etag`. The
/// outcome is left to the test, because a backend that reports no tag makes the commit throw, and
/// the writes recorded up to that point are what the test is about.
struct CommitOverExistingVersionHint
{
    using Race = ExistingVersionHintObjectStorage::Race;

    explicit CommitOverExistingVersionHint(
        const std::string & hint_etag, Race race = Race::None, std::optional<std::string> head_etag = std::nullopt)
        : object_storage(std::make_shared<ExistingVersionHintObjectStorage>(hint_etag, race, std::move(head_etag)))
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

TEST(IcebergCommitPropagation, VersionHintAppearingMidCommitKeepsThePublishedMetadataFile)
{
    /// The hint is absent when the commit checks for it, and by the time the commit tries to create
    /// it another writer has, so the exclusive create loses and the retry reads a hint that carries
    /// no tag. The metadata file is published by then, and another writer that resolves the table by
    /// listing may already have built its own snapshot on top of it. So the metadata file stays, and
    /// the commit must not fail in the ordinary way, which would make its caller clean up the files
    /// it references: the status is reported as unknown instead.
    CommitOverExistingVersionHint commit("", CommitOverExistingVersionHint::Race::CreatedBeforeTheExclusiveCreate);

    try
    {
        bool committed = commit.run();
        FAIL() << "Expected the commit to report an unknown status, got " << committed;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_STATUS_OF_INSERT) << e.message();
        EXPECT_TRUE(Iceberg::isUnknownCommitStatus());
    }

    /// The metadata file, then the exclusive create of the hint that lost the race.
    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[0].path.ends_with("v2.metadata.json")) << writes[0].path;
    EXPECT_EQ(writes[0].write_if_none_match, "*");
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_EQ(writes[1].write_if_none_match, "*");

    EXPECT_TRUE(commit.removed().empty());
    EXPECT_FALSE(commit.object_storage->metadataRemovalAttempted());
    EXPECT_TRUE(commit.object_storage->metadataPresent());
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

TEST(IcebergCommitPropagation, CreatedVersionHintIsKeptWhenItHasAnETag)
{
    /// The control for the test below: a hint this commit creates is kept when the backend reports a
    /// tag for it, because the next commit can advance it under that tag.
    CommitOverExistingVersionHint commit("\"abc\"", CommitOverExistingVersionHint::Race::CreatedByThisCommit);
    EXPECT_TRUE(commit.run());

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[0].path.ends_with("v2.metadata.json")) << writes[0].path;
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_EQ(writes[1].write_if_none_match, "*");
    EXPECT_TRUE(commit.removed().empty());
}

TEST(IcebergCommitPropagation, CreatedVersionHintWithoutETagIsTakenBackAndTheCommitKept)
{
    /// The exclusive create of the hint succeeds, but the backend reports no tag for it. Keeping it
    /// would make the table uncommittable: the next commit refuses to advance a hint it cannot
    /// compare-and-swap. So the hint is removed. The metadata file is not: it has been visible to
    /// listing-based readers since it was written, and another writer may already have committed a
    /// snapshot that inherits the manifests and data files it references (without ever looking at
    /// the hint). Removing it and failing the commit, so that the caller cleans up those files, would
    /// corrupt that snapshot. The commit succeeds without a hint, as with `iceberg_use_version_hint = 0`.
    CommitOverExistingVersionHint commit("", CommitOverExistingVersionHint::Race::CreatedByThisCommit);
    EXPECT_TRUE(commit.run());

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[0].path.ends_with("v2.metadata.json")) << writes[0].path;
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;

    const auto & removed = commit.removed();
    ASSERT_EQ(removed.size(), 1u);
    EXPECT_EQ(removed[0], writes[1].path);

    EXPECT_FALSE(commit.object_storage->hintPresent());
    EXPECT_FALSE(commit.object_storage->metadataRemovalAttempted());
    EXPECT_TRUE(commit.object_storage->metadataPresent());
}

TEST(IcebergCommitPropagation, CreatedVersionHintIsJudgedByTheTagTheNextCommitReads)
{
    /// An Azure-compatible endpoint may omit the optional `ETag` on a metadata-only request while
    /// still returning it on a read. The next commit reads the hint, so it can advance it under that
    /// tag, and the hint this commit has created must be kept.
    CommitOverExistingVersionHint commit(
        "\"abc\"", CommitOverExistingVersionHint::Race::CreatedByThisCommit, /*head_etag=*/ std::string{});
    EXPECT_TRUE(commit.run());

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_TRUE(commit.removed().empty());
}

TEST(IcebergCommitPropagation, CreatedVersionHintIsKeptWhenItsCheckFails)
{
    /// The exclusive create of the hint succeeds, but reading it back for its tag fails. Nothing is
    /// known about the hint then: another writer may already have advanced it and published a
    /// metadata file on top of this one. Removing either would break the table, so both files stay.
    /// The snapshot is published, so the commit succeeds: an error here would make the caller
    /// clean up the data files, manifests and manifest list the retained metadata references.
    CommitOverExistingVersionHint commit("\"abc\"", CommitOverExistingVersionHint::Race::CreatedByThisCommit);
    commit.object_storage->read_fails = true;
    EXPECT_TRUE(commit.run());

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_TRUE(commit.removed().empty());
}

TEST(IcebergCommitPropagation, CreatedVersionHintThatCannotBeRemovedKeepsItsMetadataFile)
{
    /// The created hint has no tag, but removing it fails. It may still point at the metadata file,
    /// so the metadata file must stay, and the commit, whose snapshot remains published, succeeds -
    /// otherwise the caller would clean up the files that snapshot references.
    CommitOverExistingVersionHint commit("", CommitOverExistingVersionHint::Race::CreatedByThisCommit);
    commit.object_storage->hint_removal_failure = ExistingVersionHintObjectStorage::RemovalFailure::BeforeApplying;
    EXPECT_TRUE(commit.run());

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_TRUE(commit.removed().empty());
}

TEST(IcebergCommitPropagation, CreatedVersionHintWhoseRemovalLostItsResponseKeepsTheCommit)
{
    /// The removal of the created hint takes effect, but its response is lost. Either way the
    /// metadata file stays and the commit succeeds.
    CommitOverExistingVersionHint commit("", CommitOverExistingVersionHint::Race::CreatedByThisCommit);
    commit.object_storage->hint_removal_failure = ExistingVersionHintObjectStorage::RemovalFailure::AfterApplying;
    EXPECT_TRUE(commit.run());

    EXPECT_EQ(commit.writes().size(), 2u);
    EXPECT_FALSE(commit.object_storage->hintPresent());
    EXPECT_FALSE(commit.object_storage->metadataRemovalAttempted());
    EXPECT_TRUE(commit.object_storage->metadataPresent());
}

TEST(IcebergCommitPropagation, CreatedVersionHintOfCreateThatCannotBeRemovedKeepsItsMetadataFile)
{
    /// The same for the hint of `CREATE TABLE`: nothing references the first metadata file yet, so
    /// the refusal propagates, but the metadata file stays while the hint may still point at it.
    auto object_storage = std::make_shared<ExistingVersionHintObjectStorage>("");
    object_storage->hint_removal_failure = ExistingVersionHintObjectStorage::RemovalFailure::BeforeApplying;

    try
    {
        Iceberg::takeBackVersionHintWithoutETag(
            object_storage,
            "/table/metadata/version-hint.text",
            /*version=*/ 1,
            {"/table/metadata/v1.metadata.json"},
            getContext().context,
            /*no_concurrent_writer=*/ true);
        FAIL() << "Expected the refusal of a version hint without an ETag";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD) << e.message();
    }

    EXPECT_TRUE(object_storage->removed.empty());
}

TEST(IcebergCommitPropagation, CreatedVersionHintAdvancedByAnotherWriterIsLeftAlone)
{
    /// The exclusive create of the hint succeeds, and another writer advances it before this commit
    /// reads it back - the read reports no tag only because this endpoint omits it now and then.
    /// The hint is no longer the one this commit created, and the metadata file of the other writer
    /// builds on the one published here, so taking either back would break the table. The commit
    /// has succeeded.
    CommitOverExistingVersionHint commit("", CommitOverExistingVersionHint::Race::CreatedByThisCommit);
    commit.object_storage->advanced_by_another_writer_to = "3";
    EXPECT_TRUE(commit.run());

    const auto & writes = commit.writes();
    ASSERT_EQ(writes.size(), 2u);
    EXPECT_TRUE(writes[1].path.ends_with("version-hint.text")) << writes[1].path;
    EXPECT_TRUE(commit.removed().empty());
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
