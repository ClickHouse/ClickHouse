#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Disks/DiskFactory.h>

namespace DB
{

#if USE_AZURE_BLOB_STORAGE

void registerDiskBlobStorage(DiskFactory &) {};

#else

void registerDiskBlobStorage(DiskFactory &) {};

#endif

}
