#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergObjectMetadataCache.h>

namespace DB::Iceberg
{

IcebergObjectMetadataCachePtr getObjectMetadataCache()
{
    static auto cache = std::make_shared<IcebergObjectMetadataCache>(
        "SLRU", 100000 * 256, 100000, 0.5);
    return cache;
}

}

#endif
