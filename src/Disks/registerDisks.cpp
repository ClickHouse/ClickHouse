#include <Disks/registerDisks.h>

#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>

#include "config.h"

namespace DB
{

void registerDiskLocal(DiskFactory & factory, bool global_skip_access_check);

#if USE_SSL
void registerDiskEncrypted(DiskFactory & factory, bool global_skip_access_check);
#endif

void registerDiskCache(DiskFactory & factory, bool global_skip_access_check);
void registerDiskObjectStorage(DiskFactory & factory, bool global_skip_access_check);


void registerDisks(bool global_skip_access_check)
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory, global_skip_access_check);

#if USE_SSL
    registerDiskEncrypted(factory, global_skip_access_check);
#endif

    registerDiskCache(factory, global_skip_access_check);

    registerDiskObjectStorage(factory, global_skip_access_check);
}

void clearDiskRegistry()
{
    DiskFactory::instance().clearRegistry();
    ObjectStorageFactory::instance().clearRegistry();
    MetadataStorageFactory::instance().clearRegistry();
}

}
