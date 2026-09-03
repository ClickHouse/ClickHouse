#include <string>
#include <vector>
#include <Common/logger_useful.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/storage/blobs.hpp>
#include <azure/storage/common/internal/xml_wrapper.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/blob_options.hpp>

#include <gtest/gtest.h>

TEST(AzureXMLWrapper, TestLeak)
{
    std::string str = "<hello>world</hello>";

    Azure::Storage::_internal::XmlReader reader(str.c_str(), str.length());
    Azure::Storage::_internal::XmlReader reader2(std::move(reader));
    Azure::Storage::_internal::XmlReader reader3 = std::move(reader2);
    reader3.Read();
}

TEST(AzureBlobContainerClient, CurlMemoryLeak)
{
    using Azure::Storage::Blobs::BlobContainerClient;
    using Azure::Storage::Blobs::BlobClientOptions;

    static constexpr auto unavailable_url = "http://unavailable:19999/bucket";
    static constexpr auto container = "container";

    BlobClientOptions options;
    options.Retry.MaxRetries = 0;

    auto client = std::make_unique<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(unavailable_url, container, options));
    EXPECT_THROW({ client->ListBlobs(); }, Azure::Core::RequestFailedException);
}

#endif
