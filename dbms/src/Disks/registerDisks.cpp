#include "DiskFactory.h"

namespace DB
{
void registerDiskLocal(DiskFactory & factory);

void registerDisks()
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory);
}

}
