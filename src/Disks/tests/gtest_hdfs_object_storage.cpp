#include "config.h"

#if USE_HDFS

#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Common/Exception.h>
#include <Poco/Util/MapConfiguration.h>

namespace DB::ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

using DB::HDFSObjectStorage;

/// `HDFSObjectStorage::makeObjectMetadata` builds the `ObjectMetadata` that the HDFS
/// `tryGetObjectMetadata` / `listObjects` paths return. A live NameNode cannot run in a
/// unit test, so this exercises the pure construction logic instead. The safety-critical
/// contract is that the synthesised etag, while populated (for the `_etag` virtual
/// column), is weak and therefore must never be usable as a content-cache key.

TEST(HDFSObjectStorageMetadata, EtagIsNeverUsableAsCacheKey)
{
    auto metadata = HDFSObjectStorage::makeObjectMetadata(/*last_modified=*/ 1700000000, /*size=*/ 42);
    EXPECT_FALSE(metadata.etag_is_strong);
    EXPECT_FALSE(metadata.isEtagUsableAsCacheKey());
}

TEST(HDFSObjectStorageMetadata, EtagIsPopulatedForTheVirtualColumn)
{
    /// The token must be non-empty so the `_etag` virtual column is populated on HDFS,
    /// even though it cannot key a cache.
    auto metadata = HDFSObjectStorage::makeObjectMetadata(/*last_modified=*/ 1700000000, /*size=*/ 42);
    EXPECT_FALSE(metadata.etag.empty());
}

TEST(HDFSObjectStorageMetadata, SizeAndModificationTimeArePreserved)
{
    auto metadata = HDFSObjectStorage::makeObjectMetadata(/*last_modified=*/ 1700000000, /*size=*/ 42);
    EXPECT_EQ(metadata.size_bytes, 42u);
    EXPECT_EQ(metadata.last_modified.epochTime(), 1700000000);
}

namespace
{

/// `HDFSObjectStorage` keeps the configuration by reference and hands that reference to every write
/// buffer it opens, so the configuration must outlive the storage. All tests here need the same
/// immutable configuration, so one instance with static storage duration serves them all and can
/// never dangle.
const Poco::Util::AbstractConfiguration & unreachableConfig()
{
    static const Poco::AutoPtr<Poco::Util::MapConfiguration> config = []
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> result(new Poco::Util::MapConfiguration());
        /// Only the negative control reaches the NameNode, and it is expected to fail. Every connect
        /// attempt sleeps a second before giving up (`contrib/libhdfs3/src/rpc/RpcChannel.cpp:328`), so
        /// cap the attempts at the minimum the setting accepts; the default is 10
        /// (`contrib/libhdfs3/src/common/SessionConfig.cpp:90`). `_` becomes `.` in
        /// `HDFSBuilderWrapper::loadFromConfig`. Two attempts remain, because the probe RPC that
        /// finishes the connection is idempotent and so is retried once
        /// (`RpcChannel.cpp:441-442`), which is what the control's ~2s consists of.
        result->setString("hdfs.rpc_client_connect_retry", "1");
        return result;
    }();
    return *config;
}

/// `lazy_initialize = true` defers `initializeHDFSFS`, so the storage is constructible against an
/// unreachable NameNode. The conditional-write refusal is checked before that initialisation, which
/// is what makes it observable here; reaching any later statement requires a live NameNode.
std::unique_ptr<HDFSObjectStorage> makeUnreachableStorage()
{
    return std::make_unique<HDFSObjectStorage>(
        "hdfs://localhost:1/data/",
        std::make_unique<DB::HDFSObjectStorageSettings>(/*min_bytes_for_seek_=*/ 1024, /*replication_=*/ 1),
        unreachableConfig(),
        /*lazy_initialize=*/ true);
}

}

/// A conditional write is a compare-and-swap request. `HDFSObjectStorage` refuses it because only the
/// `If-None-Match` half is expressible on HDFS, and honouring one half would leave the other silently
/// degraded (see the guard's comment for the derivation). These tests pin the refusal.

TEST(HDFSObjectStorageConditionalWrite, RefusesIfNoneMatch)
{
    auto object_storage = makeUnreachableStorage();
    DB::WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "*";

    try
    {
        object_storage->writeObject(
            DB::StoredObject("hdfs://localhost:1/data/metadata/v2.metadata.json"),
            DB::WriteMode::Rewrite,
            /*attributes=*/ {},
            DB::DBMS_DEFAULT_BUFFER_SIZE,
            write_settings);
        FAIL() << "Expected the conditional write to be refused";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNSUPPORTED_METHOD) << e.message();
        /// The message names the object, so an operator can tell which commit was refused.
        EXPECT_NE(e.message().find("v2.metadata.json"), std::string::npos) << e.message();
    }
}

TEST(HDFSObjectStorageConditionalWrite, RefusesIfMatch)
{
    auto object_storage = makeUnreachableStorage();
    DB::WriteSettings write_settings;
    write_settings.object_storage_write_if_match = "some-etag";

    try
    {
        object_storage->writeObject(
            DB::StoredObject("hdfs://localhost:1/data/metadata/version-hint.text"),
            DB::WriteMode::Rewrite,
            /*attributes=*/ {},
            DB::DBMS_DEFAULT_BUFFER_SIZE,
            write_settings);
        FAIL() << "Expected the conditional write to be refused";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNSUPPORTED_METHOD) << e.message();
        EXPECT_NE(e.message().find("version-hint.text"), std::string::npos) << e.message();
    }
}

TEST(HDFSObjectStorageConditionalWrite, DoesNotRefuseUnconditionalWrite)
{
    /// Negative control: with no condition there is nothing to refuse, so the write proceeds past the
    /// guard and then fails reaching the unreachable NameNode. That failure must NOT be
    /// `UNSUPPORTED_METHOD`, otherwise the two tests above would also pass against a guard that
    /// rejects every HDFS write -- which is what makes them meaningful.
    auto object_storage = makeUnreachableStorage();
    DB::WriteSettings write_settings;
    ASSERT_TRUE(write_settings.object_storage_write_if_none_match.empty());
    ASSERT_TRUE(write_settings.object_storage_write_if_match.empty());

    try
    {
        object_storage->writeObject(
            DB::StoredObject("hdfs://localhost:1/data/metadata/v2.metadata.json"),
            DB::WriteMode::Rewrite,
            /*attributes=*/ {},
            DB::DBMS_DEFAULT_BUFFER_SIZE,
            write_settings);
    }
    catch (const DB::Exception & e)
    {
        EXPECT_NE(e.code(), DB::ErrorCodes::UNSUPPORTED_METHOD) << e.message();
    }
}

#endif
