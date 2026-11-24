#include <gtest/gtest.h>

#include <config.h>

#if USE_AVRO

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/Logger.h>

#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>

namespace fs = std::filesystem;
using namespace DB;

namespace DB::ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

struct ScopedTempDir
{
    fs::path path;

    explicit ScopedTempDir(const std::string & name)
        : path(fs::temp_directory_path() / (name + "_" + std::to_string(::getpid())))
    {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }

    ~ScopedTempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

class CountingLocalObjectStorage : public LocalObjectStorage
{
public:
    using LocalObjectStorage::LocalObjectStorage;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override
    {
        ++list_calls;
        /// An object that appears between two attempts: only the listing that decided the throw can report it.
        if (!reveal_path.empty() && list_calls.load() == reveal_on_call)
            std::ofstream(reveal_path, std::ios::binary | std::ios::trunc);
        LocalObjectStorage::listObjects(path, children, max_keys);
        /// `S3ObjectStorage::listObjects` passes `path` as the `ListObjectsV2` `Prefix` and reports keys
        /// verbatim, so an object keyed exactly `path` is listed. A local directory cannot hold one.
        if (report_prefix_as_object)
        {
            ObjectMetadata object_metadata;
            object_metadata.last_modified = Poco::Timestamp::fromEpochTime(1000000000);
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(path, object_metadata));
        }
    }

    mutable std::atomic<size_t> list_calls = 0;
    std::string reveal_path;
    size_t reveal_on_call = 0;
    bool report_prefix_as_object = false;
};

}

TEST(IcebergMetadataListingRequests, EmptyMetadataPrefixIsListedOncePerAttempt)
{
    ScopedTempDir temporary_directory("ch_gtest_iceberg_metadata_listing_requests");
    auto table = temporary_directory.path / "default" / "test_table";
    const String metadata_prefix = (table / "metadata").string();
    fs::create_directories(metadata_prefix);

    auto object_storage = std::make_shared<CountingLocalObjectStorage>(LocalObjectStorageSettings(
        "test_iceberg_metadata_listing_requests", temporary_directory.path.string(), /*read_only_=*/false));

    DataLakeStorageSettings settings;

    try
    {
        Iceberg::getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            table.string(),
            settings,
            /*metadata_cache=*/nullptr,
            getContext().context,
            &Poco::Logger::get("IcebergMetadataListingRequestsTest"),
            /*table_uuid=*/std::nullopt,
            CompressionMethod::None);
        FAIL() << "Expected FILE_DOESNT_EXIST";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_DOESNT_EXIST);
        EXPECT_NE(e.message().find("no .metadata.json under " + metadata_prefix + ", which held 0 entries"), String::npos)
            << e.message();
    }

    /// `5` is the file-local `MAX_LIST_RETRIES` of `Utils.cpp`; the message describes a listing already made.
    EXPECT_EQ(object_storage->list_calls.load(), 5u);
}

TEST(IcebergMetadataListingRequests, ReportsTheListingThatDecidedTheThrow)
{
    ScopedTempDir temporary_directory("ch_gtest_iceberg_metadata_listing_deciding");
    auto table = temporary_directory.path / "default" / "test_table";
    fs::create_directories(table / "metadata");

    auto object_storage = std::make_shared<CountingLocalObjectStorage>(LocalObjectStorageSettings(
        "test_iceberg_metadata_listing_deciding", temporary_directory.path.string(), /*read_only_=*/false));
    /// Appears only for the last of the `MAX_LIST_RETRIES` attempts, and never matches `.metadata.json`.
    object_storage->reveal_path = (table / "metadata" / "late-arrival.text").string();
    object_storage->reveal_on_call = 5;

    DataLakeStorageSettings settings;

    try
    {
        Iceberg::getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            table.string(),
            settings,
            /*metadata_cache=*/nullptr,
            getContext().context,
            &Poco::Logger::get("IcebergMetadataListingRequestsTest"),
            /*table_uuid=*/std::nullopt,
            CompressionMethod::None);
        FAIL() << "Expected FILE_DOESNT_EXIST";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_DOESNT_EXIST);
        const String & message = e.message();
        EXPECT_NE(message.find("which held 1 entry: /late-arrival.text"), String::npos) << message;
    }

    EXPECT_EQ(object_storage->list_calls.load(), 5u);
}

TEST(IcebergMetadataListingRequests, ReportsAnObjectKeyedExactlyAsThePrefix)
{
    ScopedTempDir temporary_directory("ch_gtest_iceberg_metadata_listing_prefix_key");
    auto table = temporary_directory.path / "default" / "test_table";
    fs::create_directories(table / "metadata");

    auto object_storage = std::make_shared<CountingLocalObjectStorage>(LocalObjectStorageSettings(
        "test_iceberg_metadata_listing_prefix_key", temporary_directory.path.string(), /*read_only_=*/false));
    object_storage->report_prefix_as_object = true;

    DataLakeStorageSettings settings;

    try
    {
        Iceberg::getLatestOrExplicitMetadataFileAndVersion(
            object_storage,
            table.string(),
            settings,
            /*metadata_cache=*/nullptr,
            getContext().context,
            &Poco::Logger::get("IcebergMetadataListingRequestsTest"),
            /*table_uuid=*/std::nullopt,
            CompressionMethod::None);
        FAIL() << "Expected FILE_DOESNT_EXIST";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_DOESNT_EXIST);
        const String & message = e.message();
        const String listed_prefix = (table / "metadata").string();
        EXPECT_NE(
            message.find("which held 1 entry: " + listed_prefix + " (2001-09-09T01:46:40Z)"), String::npos) << message;
    }

    EXPECT_EQ(object_storage->list_calls.load(), 5u);
}

#endif
