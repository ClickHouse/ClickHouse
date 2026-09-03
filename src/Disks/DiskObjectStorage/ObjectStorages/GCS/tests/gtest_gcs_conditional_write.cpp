#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

#include <google/cloud/credentials.h>
#include <google/cloud/storage/client.h>

using namespace DB;

namespace gcs = ::google::cloud::storage;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// A conditional write (`object_storage_write_if_none_match` / `object_storage_write_if_match`) is a
/// compare-and-swap request. The native GCS backend expresses it through generation preconditions,
/// so a request it cannot translate must be rejected before any byte is written — an unconditional
/// write in its place would silently discard one of two concurrent writers. These tests cover the
/// rejection paths, which throw before the client contacts any server.

namespace
{

std::shared_ptr<gcs::Client> makeOfflineClient()
{
    /// Insecure credentials and an unreachable endpoint: client construction performs no I/O, and
    /// the rejection paths throw before any request is issued.
    /// Connection refused counts as a transient error, so the default retry policy would keep
    /// retrying the unreachable endpoint for minutes; cap it so the accepted-precondition cases
    /// fail fast instead.
    auto options = google::cloud::Options{}
        .set<google::cloud::UnifiedCredentialsOption>(google::cloud::MakeInsecureCredentials())
        .set<gcs::RestEndpointOption>("http://127.0.0.1:1")
        .set<gcs::RetryPolicyOption>(gcs::LimitedErrorCountRetryPolicy(0).clone());
    return std::make_shared<gcs::Client>(std::move(options));
}

int conditionalWriteErrorCode(const WriteSettings & write_settings)
{
    try
    {
        WriteBufferFromGCS buffer(makeOfflineClient(), "bucket", "key", 1024, write_settings, nullptr);
        return 0;
    }
    catch (const Exception & e)
    {
        return e.code();
    }
}

}

TEST(GCSConditionalWrite, BothPreconditionsRejected)
{
    WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "*";
    write_settings.object_storage_write_if_match = "123";
    EXPECT_EQ(conditionalWriteErrorCode(write_settings), ErrorCodes::BAD_ARGUMENTS);
}

TEST(GCSConditionalWrite, IfNoneMatchAcceptsOnlyStar)
{
    WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "some-etag";
    EXPECT_EQ(conditionalWriteErrorCode(write_settings), ErrorCodes::BAD_ARGUMENTS);
}

TEST(GCSConditionalWrite, IfMatchRequiresAGeneration)
{
    /// This backend's etag is the object generation (a decimal number); an etag that cannot be a
    /// generation came from somewhere else and cannot be enforced.
    WriteSettings write_settings;
    write_settings.object_storage_write_if_match = "not-a-generation";
    EXPECT_EQ(conditionalWriteErrorCode(write_settings), ErrorCodes::BAD_ARGUMENTS);
}

TEST(GCSConditionalWrite, TranslatablePreconditionsAreAccepted)
{
    /// Valid preconditions reach the SDK; errors from the (unreachable) endpoint surface later,
    /// on write/finalize, as stream failures — never as a constructor throw.
    {
        WriteSettings write_settings;
        write_settings.object_storage_write_if_none_match = "*";
        EXPECT_EQ(conditionalWriteErrorCode(write_settings), 0);
    }
    {
        WriteSettings write_settings;
        write_settings.object_storage_write_if_match = "12345";
        EXPECT_EQ(conditionalWriteErrorCode(write_settings), 0);
    }
}

#endif
