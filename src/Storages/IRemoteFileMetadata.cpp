#include <Storages/IRemoteFileMetadata.h>
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

IRemoteFileMetadata::~IRemoteFileMetadata() {}

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

void RemoteFileMetadataFactory::registerClass(const String & name, ClassCreator creator)
{
    auto it = class_creators.find(name);
    if (it != class_creators.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Class ({}) has been registered. It is a fatal error.", name);
    }
    class_creators[name] = creator;
}
}
