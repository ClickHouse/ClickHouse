#include <gtest/gtest.h>
#include <Disks/BlobStorage/DiskBlobStorage.h>

#if USE_AZURE_BLOB_STORAGE

using namespace DB;

TEST(DiskBlobStorage, demo)
{
    blob_storage_demo();
}

#endif
