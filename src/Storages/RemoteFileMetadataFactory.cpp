#include <Storages/RemoteFileMetadataFactory.h>
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
    auto it = class_creators.find(name);
    if (it == class_creators.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found metadata class:{}", name);
    return (it->second)();
}

void RemoteFileMetadataFactory::registerRemoteFileMatadataCreator(const String & name, MetadataCreator creator)
{
    auto it = class_creators.find(name);
    if (it != class_creators.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Class ({}) has been registered. It is a fatal error.", name);
    }
    class_creators[name] = creator;
}

void registerStorageHiveMetadataCreator();

void registerRemoteFileMatadataCreators()
{
    registerStorageHiveMetadataCreator();
}
}
