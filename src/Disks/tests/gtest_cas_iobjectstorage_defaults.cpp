#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <IO/ObjectStorageRequestProfile.h>

/// `IObjectStorage::removeObjectsIfExistUnderProfile` has three siblings (`iterate`,
/// `tryGetObjectMetadataWithNativeToken`, `removeObjectIfTokenMatches`) whose defaults all forward a
/// Default-profile request to the plain, no-profile method and refuse only SingleAttempt. This file
/// pins that `removeObjectsIfExistUnderProfile` follows the same rule, using a minimal stub storage
/// that implements nothing beyond what `IObjectStorage` requires.

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Implements only what `IObjectStorage` declares pure; every method a case below does not exercise
/// throws if called, so a test that reaches one it did not expect fails loudly instead of silently
/// doing the wrong thing.
class MinimalObjectStorage : public IObjectStorage
{
public:
    std::string getName() const override
    {
        return "MinimalObjectStorage";
    }

    ObjectStorageType getType() const override
    {
        return ObjectStorageType::None;
    }

    std::string getCommonKeyPrefix() const override
    {
        return "";
    }

    std::string getDescription() const override
    {
        return "MinimalObjectStorage (test stub)";
    }

    bool exists(const StoredObject &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not used by this test");
    }

    ObjectMetadata getObjectMetadata(const std::string &, bool) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not used by this test");
    }

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string &, bool) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not used by this test");
    }

    std::unique_ptr<ReadBufferFromFileBase> readObject(
        const StoredObject &, const ReadSettings &, std::optional<size_t>, bool, bool) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not used by this test");
    }

    std::unique_ptr<WriteBufferFromFileBase> writeObject(
        const StoredObject &, WriteMode, std::optional<ObjectAttributes>, size_t, const WriteSettings &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not used by this test");
    }

    bool isRemote() const override
    {
        return true;
    }

    void removeObjectIfExists(const StoredObject &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not used by this test");
    }

    /// The method under test: `removeObjectsIfExistUnderProfile`'s Default-profile default forwards here.
    void removeObjectsIfExist(const StoredObjects & objects) override
    {
        ++remove_objects_if_exist_calls;
        last_removed_objects = objects;
    }

    void copyObject(
        const StoredObject &, const StoredObject &, const ReadSettings &, const WriteSettings &, std::optional<ObjectAttributes>) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not used by this test");
    }

    void shutdown() override
    {
    }

    void startup() override
    {
    }

    String getObjectsNamespace() const override
    {
        return "";
    }

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override
    {
        return nullptr;
    }

    size_t remove_objects_if_exist_calls = 0;
    StoredObjects last_removed_objects;
};

}

TEST(CASIObjectStorageDefaults, RemoveObjectsIfExistUnderProfileDefaultForwards)
{
    MinimalObjectStorage storage;
    const StoredObjects objects{StoredObject("a"), StoredObject("b")};

    ObjectStorageControlRequest request;
    request.profile = ObjectStorageRetryProfile::Default;

    storage.removeObjectsIfExistUnderProfile(objects, request);

    EXPECT_EQ(storage.remove_objects_if_exist_calls, 1u);
    ASSERT_EQ(storage.last_removed_objects.size(), 2u);
    EXPECT_EQ(storage.last_removed_objects[0].remote_path, "a");
    EXPECT_EQ(storage.last_removed_objects[1].remote_path, "b");
}

TEST(CASIObjectStorageDefaults, RemoveObjectsIfExistUnderProfileSingleAttemptThrows)
{
    MinimalObjectStorage storage;
    const StoredObjects objects{StoredObject("a")};

    ObjectStorageControlRequest request;
    request.profile = ObjectStorageRetryProfile::SingleAttempt;

    try
    {
        storage.removeObjectsIfExistUnderProfile(objects, request);
        FAIL() << "expected a SingleAttempt batch-remove request to be refused";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
    }

    EXPECT_EQ(storage.remove_objects_if_exist_calls, 0u);
}

}
