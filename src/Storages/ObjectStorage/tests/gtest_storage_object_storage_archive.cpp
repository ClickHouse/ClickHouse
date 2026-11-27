#include <gtest/gtest.h>

#include <Interpreters/ClusterFunctionReadTask.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
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
    std::string getName() const override { return "UnknownSize"; }
    DB::ObjectStorageType getType() const override { return DB::ObjectStorageType::Web; }
    std::string getCommonKeyPrefix() const override { return ""; }
    std::string getDescription() const override { return "unknown size test storage"; }
    bool exists(const DB::StoredObject &) const override { return true; }

    DB::ObjectMetadata getObjectMetadata(const std::string &, bool) const override { return makeMetadata(); }
    DB::ObjectMetadata getObjectMetadata(const DB::RelativePathWithMetadata &, bool) const override { return makeMetadata(); }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string &, bool) const override { return makeMetadata(); }
    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const DB::RelativePathWithMetadata &, bool) const override { return makeMetadata(); }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
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

    bool isRemote() const override { return true; }
    void removeObjectIfExists(const DB::StoredObject &) override {}
    void removeObjectsIfExist(const DB::StoredObjects &) override {}

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
    std::string getObjectsNamespace() const override { return ""; }
    DB::ObjectStorageKeyGeneratorPtr createKeyGenerator() const override { return nullptr; }

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

TEST(StorageObjectStorageArchive, DistributedArchiveRejectsUnknownSize)
{
    bool sent_task = false;
    DB::ClusterFunctionReadTaskCallback callback = [&sent_task]
    {
        if (sent_task)
            return std::make_shared<DB::ClusterFunctionReadTaskResponse>();

        sent_task = true;
        return std::make_shared<DB::ClusterFunctionReadTaskResponse>("archive.zip :: value.tsv");
    };

    auto object_storage = std::make_shared<UnknownSizeObjectStorage>();
    auto context = DB::Context::createCopy(::getContext().context);
    DB::StorageObjectStorageSource::ReadTaskIterator iterator(
        callback,
        /*max_threads_count=*/1,
        /*is_archive_=*/true,
        object_storage,
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
