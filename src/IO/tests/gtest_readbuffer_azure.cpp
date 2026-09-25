#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int NOT_INITIALIZED;
}

TEST(ReadBufferFromAzureBlobStorage, MetadataBeforeRequest)
{
    auto client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://127.0.0.1/container"), "");
    DB::ReadBufferFromAzureBlobStorage buffer(client, "blob", DB::ReadSettings{}, 1, 1);

    try
    {
        buffer.getObjectMetadataFromTheLastRequest();
        FAIL() << "Expected NOT_INITIALIZED before a successful request";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_INITIALIZED);
    }
}

#endif
