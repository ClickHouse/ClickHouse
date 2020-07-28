#include "registerDisks.h"

#include "DiskFactory.h"

#include <Common/config.h>

namespace DB
{
void registerDiskLocal(DiskFactory & factory);
void registerDiskMemory(DiskFactory & factory);
#if USE_AWS_S3
void registerDiskS3(DiskFactory & factory);
#endif

void registerDisks()
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory);
    registerDiskMemory(factory);
#if USE_AWS_S3
    registerDiskS3(factory);
#endif
}

}
