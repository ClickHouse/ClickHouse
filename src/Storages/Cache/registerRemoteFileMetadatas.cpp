#include <Storages/Cache/registerRemoteFileMetadatas.h>
#include <Storages/Cache/RemoteFileMetadataFactory.h>
#include <Common/config.h>

namespace DB
{

#if USE_HIVE
void registerStorageHiveMetadata(RemoteFileMetadataFactory & factory);
#endif

void registerRemoteFileMetadatas()
{
    [[maybe_unused]] auto & factory = RemoteFileMetadataFactory::instance();

#if USE_HIVE
    registerStorageHiveMetadata(factory);
#endif
}

}
