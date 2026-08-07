#include <Storages/Cache/RemoteFileMetadataFactory.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

RemoteFileMetadataFactory & RemoteFileMetadataFactory::instance()
{
    static RemoteFileMetadataFactory g_factory;
    return g_factory;
}

IRemoteFileMetadataPtr RemoteFileMetadataFactory::get(const String & name)
{
    auto it = remote_file_metadatas.find(name);
    if (it == remote_file_metadatas.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found metadata class:{}", name);
    return (it->second)();
}

void RemoteFileMetadataFactory::registerRemoteFileMatadata(const String & name, MetadataCreator creator)
{
    auto it = remote_file_metadatas.find(name);
    if (it != remote_file_metadatas.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata class ({}) has already been registered.", name);
    }
    remote_file_metadatas[name] = creator;
}
}
