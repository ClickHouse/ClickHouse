#include <gtest/gtest.h>
#include <Disks/BlobStorage/DiskBlobStorage.h>

#if USE_AZURE_BLOB_STORAGE

using namespace DB;

TEST(DiskBlobStorage, doAll)
{
    blob_do_sth();
}

#endif
