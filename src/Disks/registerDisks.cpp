#include "registerDisks.h"

#include "DiskFactory.h"

#include "config.h"
#include "magic_enum.hpp"

namespace DB
{

void registerDiskLocal(DiskFactory & factory, DiskStartupFlags disk_flags);

#if USE_SSL
void registerDiskEncrypted(DiskFactory & factory, DiskStartupFlags disk_flags);
#endif

void registerDiskCache(DiskFactory & factory, DiskStartupFlags disk_flags);
void registerDiskObjectStorage(DiskFactory & factory, DiskStartupFlags disk_flags);


#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD

void registerDisks(DiskStartupFlags disk_flags)
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory, disk_flags);

#if USE_SSL
    registerDiskEncrypted(factory, disk_flags);
#endif

    registerDiskCache(factory, disk_flags);

    registerDiskObjectStorage(factory, disk_flags);
}

#else

void registerDisks(DiskStartupFlags disk_flags)
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory, disk_flags);

    registerDiskObjectStorage(factory, disk_flags);
}

#endif

bool is_set(DiskStartupFlags flag1, DiskStartupFlags flag2)
{
    return (magic_enum::enum_integer(flag1) & magic_enum::enum_integer(flag2));
}

}
