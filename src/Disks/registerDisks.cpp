#include "registerDisks.h"

#include "DiskFactory.h"

#include "config.h"

namespace DB
{

void registerDiskLocal(DiskFactory & factory, bool global_skip_access_check);

#if USE_AWS_S3
void registerDiskS3(DiskFactory & factory, bool global_skip_access_check);
#endif

#if USE_AZURE_BLOB_STORAGE
void registerDiskAzureBlobStorage(DiskFactory & factory, bool global_skip_access_check);
#endif

#if USE_SSL
void registerDiskEncrypted(DiskFactory & factory, bool global_skip_access_check);
#endif

#if USE_HDFS
void registerDiskHDFS(DiskFactory & factory, bool global_skip_access_check);
#endif

void registerDiskWebServer(DiskFactory & factory, bool global_skip_access_check);

void registerDiskCache(DiskFactory & factory, bool global_skip_access_check);

void registerDisks(bool global_skip_access_check)
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory, global_skip_access_check);

#if USE_AWS_S3
    registerDiskS3(factory, global_skip_access_check);
#endif

#if USE_AZURE_BLOB_STORAGE
    registerDiskAzureBlobStorage(factory, global_skip_access_check);
#endif

#if USE_SSL
    registerDiskEncrypted(factory, global_skip_access_check);
#endif

#if USE_HDFS
    registerDiskHDFS(factory, global_skip_access_check);
#endif

    registerDiskWebServer(factory, global_skip_access_check);

    registerDiskCache(factory, global_skip_access_check);
}

}
