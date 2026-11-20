#include <gtest/gtest.h>

#include <Interpreters/ClusterFunctionReadTask.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Web/Configuration.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_UNPACK_ARCHIVE;
    extern const int LOGICAL_ERROR;
}

namespace
{
class UnknownSizeObjectStorage final : public DB::IObjectStorage
{
public:
    [[nodiscard]] std::string getName() const override { return "UnknownSize"; }
    [[nodiscard]] DB::ObjectStorageType getType() const override { return DB::ObjectStorageType::Web; }
    [[nodiscard]] std::string getCommonKeyPrefix() const override { return ""; }
    [[nodiscard]] std::string getDescription() const override { return "unknown size test storage"; }
    [[nodiscard]] bool exists(const DB::StoredObject &) const override { return true; }

    [[nodiscard]] DB::ObjectMetadata getObjectMetadata(const std::string &, bool) const override { return makeMetadata(); }
    [[nodiscard]] DB::ObjectMetadata getObjectMetadata(const DB::RelativePathWithMetadata &, bool) const override { return makeMetadata(); }

    [[nodiscard]] std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string &, bool) const override { return makeMetadata(); }
    [[nodiscard]] std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const DB::RelativePathWithMetadata &, bool) const override { return makeMetadata(); }

    [[nodiscard]] std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject &,
        const DB::ReadSettings &,
        std::optional<size_t>,
        bool,
        bool) const override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "The archive reader should not read an object of unknown size");
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject &,
        DB::WriteMode,
        std::optional<DB::ObjectAttributes>,
        size_t,
        const DB::WriteSettings &) override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected write");
    }

    [[nodiscard]] bool isRemote() const override { return true; }
    void removeObjectIfExists(const DB::StoredObject &) override {}
    void removeObjectsIfExist(
        const DB::StoredObjects &,
        DB::StoredObjects *) override {}

    void copyObject(
        const DB::StoredObject &,
        const DB::StoredObject &,
        const DB::ReadSettings &,
        const DB::WriteSettings &,
        std::optional<DB::ObjectAttributes>) override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected copy");
    }

    void shutdown() override {}
    void startup() override {}
    [[nodiscard]] std::string getObjectsNamespace() const override { return ""; }
    [[nodiscard]] DB::ObjectStorageKeyGeneratorPtr createKeyGenerator() const override { return nullptr; }

private:
    static DB::ObjectMetadata makeMetadata()
    {
        DB::ObjectMetadata metadata;
        metadata.is_size_known = false;
        metadata.size_bytes = 0;
        return metadata;
    }
};
}

/// The worker builds its configuration from the same source as the initiator, so a fixed member name is
/// known there and the task path does not have to be re-parsed. It has to be initialized, because that is
/// what parses the source into an archive and a member.
static DB::StorageObjectStorageConfigurationPtr makeWebConfiguration(const String & member = "value.tsv")
{
    auto configuration = std::make_shared<DB::StorageWebConfiguration>();
    /// The split below is what these tests are about, so it must not depend on the default of the setting.
    configuration->setAllowArchivePathSyntax(true);
    /// Only the URL: the format is not part of what the split depends on, and this test binary does not
    /// register the format names.
    DB::ASTs args{DB::make_intrusive<DB::ASTLiteral>("http://host/archive.zip :: " + member)};
    DB::StorageObjectStorageConfiguration::initialize(
        *configuration, args, ::getContext().context, /*with_table_structure=*/false);
    EXPECT_TRUE(configuration->isArchive());
    EXPECT_EQ(configuration->getPathInArchive(), member);
    return configuration;
}

TEST(StorageObjectStorageArchive, DistributedArchiveRejectsUnknownSize)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("archive.zip::value.tsv");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/true,
        /*expects_whole_archive_=*/false,
        object_storage,
        makeWebConfiguration(),
        context);

    try
    {
        (void)iterator.next(/*processor=*/0);
        FAIL() << "Expected CANNOT_UNPACK_ARCHIVE";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_UNPACK_ARCHIVE);
        EXPECT_NE(std::string_view(e.message()).find("size is unknown"), std::string_view::npos);
    }
}

/// The initiator split the archive into members, so this worker is expected to receive one task per member.
/// A worker that expects the archive whole would read every member once per task, so it refuses the task.
TEST(StorageObjectStorageArchive, DistributedArchiveRejectsAPerMemberTask)
{
    for (const auto & task_path : {String("archive.zip::value.tsv"), String("archive.zip?x=1::value.tsv")})
    {
        bool sent_task = false;
        DB::ClusterFunctionReadTaskCallback callback = [&sent_task, &task_path]
        {
            if (sent_task)
                return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

            sent_task = true;
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>(task_path);
        };

        auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
        auto context = DB::Context::createCopy(::getContext().context);
        DB::StorageObjectStorageSource::ReadTaskIterator iterator(
            callback,
            /*max_threads_count=*/1,
            /*is_archive_=*/false,
            /*expects_whole_archive_=*/true,
            object_storage,
            makeWebConfiguration(),
            context);

        try
        {
            (void)iterator.next(/*processor=*/0);
            FAIL() << "Expected CANNOT_UNPACK_ARCHIVE for " << task_path;
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_UNPACK_ARCHIVE);
            EXPECT_NE(std::string_view(e.message()).find("does not match the initiator"), std::string_view::npos);
        }
    }
}

/// A member glob names a concrete member the configuration does not know, so the worker falls back to
/// splitting the task path. A whole-archive task whose outer object name contains `::` is then reported, and
/// the message has to say that the two causes cannot be told apart.
TEST(StorageObjectStorageArchive, DistributedArchiveReportsTheGlobMemberAmbiguity)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("data/x.zip::y.zip");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/false,
        /*expects_whole_archive_=*/true,
        object_storage,
        makeWebConfiguration("*.csv"),
        context);

    try
    {
        (void)iterator.next(/*processor=*/0);
        FAIL() << "Expected CANNOT_UNPACK_ARCHIVE";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_UNPACK_ARCHIVE);
        EXPECT_NE(std::string_view(e.message()).find("cannot be told apart"), std::string_view::npos);
    }
}

/// An archive member is keyed by its container's ETag, which a Web server may report weakly, so it makes no
/// query-condition-cache key at all: a stale entry would skip rows of a container that was overwritten
/// within the same second and with the same size. The same fail-close the TopK path applies to archives.
TEST(StorageObjectStorageArchive, ArchiveMemberMakesNoQueryConditionCacheKey)
{
    DB::ObjectMetadata metadata;
    metadata.etag = "\"v1\"";
    metadata.etag_is_strong = true;

    DB::ObjectInfo plain_object(DB::RelativePathWithMetadata("data/part0.tsv", metadata));

    /// A plain object with a strong ETag does make a key, so the fixture would catch a guard that never fires.
    ASSERT_TRUE(DB::StorageObjectStorageSource::makeQueryConditionCacheKey(
        "data/part0.tsv", plain_object, /*is_data_lake=*/false).has_value());

    DB::StorageObjectStorageSource::ArchiveIterator::ObjectInfoInArchive member_object(
        std::make_shared<DB::ObjectInfo>(DB::RelativePathWithMetadata("data/part0.zip")),
        "value.tsv",
        /*archive_reader=*/nullptr,
        DB::IArchiveReader::FileInfo{},
        std::nullopt);

    /// The key is computed from the member's own metadata, so the fixture has to carry one: with an empty
    /// metadata the key would be absent for a second reason, and dropping the archive guard would not fail
    /// this test.
    member_object.setObjectMetadata(metadata);

    EXPECT_FALSE(DB::StorageObjectStorageSource::makeQueryConditionCacheKey(
        "data/part0.zip::value.tsv", member_object, /*is_data_lake=*/false).has_value());
}

/// A member that itself contains `::` keeps it: the first separator is the one the initiator used, so the
/// split has to happen there and not at the last one.
TEST(StorageObjectStorageArchive, DistributedArchiveKeepsTheSeparatorInsideAMember)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("data/part0.zip::dir::file.csv");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/true,
        /*expects_whole_archive_=*/false,
        object_storage,
        makeWebConfiguration("*.csv"),
        context);

    /// The archive is `data/part0.zip` and the member keeps its own `::`; the read then fails on the
    /// archive itself, which is what the fixture storage reports for an unknown size. The message names the
    /// archive, so a split at the last separator - which would report `data/part0.zip::dir` - fails here.
    try
    {
        (void)iterator.next(/*processor=*/0);
        FAIL() << "Expected CANNOT_UNPACK_ARCHIVE";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_UNPACK_ARCHIVE);
        EXPECT_NE(std::string_view(e.message()).find("data/part0.zip"), std::string_view::npos) << e.message();
        EXPECT_EQ(std::string_view(e.message()).find("data/part0.zip::dir"), std::string_view::npos) << e.message();
    }
}

/// A member glob names a concrete member the configuration does not know, so the split is checked against
/// the pattern: a split that lands inside the name would read a different object than the initiator
/// enumerated.
TEST(StorageObjectStorageArchive, DistributedArchiveRejectsAMemberOutsideThePattern)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("archive.zip::a::b");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/true,
        /*expects_whole_archive_=*/false,
        object_storage,
        makeWebConfiguration("*.csv"),
        context);

    try
    {
        (void)iterator.next(/*processor=*/0);
        FAIL() << "Expected CANNOT_UNPACK_ARCHIVE";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_UNPACK_ARCHIVE);
        EXPECT_NE(std::string_view(e.message()).find("does not match the configured pattern"), std::string_view::npos);
    }
}

/// A whole-archive task that does not look like a per-member one is passed through with a glob member too.
TEST(StorageObjectStorageArchive, DistributedArchiveAcceptsAWholeArchiveTaskWithAGlobMember)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("data/x.zip");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/false,
        /*expects_whole_archive_=*/true,
        object_storage,
        makeWebConfiguration("*.csv"),
        context);

    auto object_info = iterator.next(/*processor=*/0);
    ASSERT_NE(object_info, nullptr);
    EXPECT_EQ(object_info->getPath(), "data/x.zip");
}

/// An outer object name may contain `::` itself. The task names the configured member after `::`, so an
/// object key that merely contains `::` is not mistaken for a per-member task.
TEST(StorageObjectStorageArchive, DistributedArchiveAcceptsAnObjectNameWithSeparator)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("data/x.zip::y.zip");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/false,
        /*expects_whole_archive_=*/true,
        object_storage,
        makeWebConfiguration(),
        context);

    /// The task is passed through as the outer object it names, not refused as a per-member one.
    auto object_info = iterator.next(/*processor=*/0);
    ASSERT_NE(object_info, nullptr);
    EXPECT_EQ(object_info->getPath(), "data/x.zip::y.zip");
}

/// The initiator sent the archive whole, so this worker is expected to unpack it. A worker that would not is
/// one whose `cluster_function_process_archive_on_multiple_nodes` disagrees with the initiator's, and reading
/// the archive bytes as data would be worse than refusing the task.
TEST(StorageObjectStorageArchive, DistributedArchiveRejectsAWholeArchiveTask)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("archive.zip");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/true,
        /*expects_whole_archive_=*/false,
        object_storage,
        makeWebConfiguration(),
        context);

    try
    {
        (void)iterator.next(/*processor=*/0);
        FAIL() << "Expected CANNOT_UNPACK_ARCHIVE";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_UNPACK_ARCHIVE);
        EXPECT_NE(std::string_view(e.message()).find("disagree"), std::string_view::npos);
    }
}
