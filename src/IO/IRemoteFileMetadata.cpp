#include <IO/IRemoteFileMetadata.h>
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IRemoteFileMetadata::~IRemoteFileMetadata() {}

RemoteFileMetadataFactory & RemoteFileMetadataFactory::instance()
{
    static RemoteFileMetadataFactory g_factory;
    return g_factory;
}

IRemoteFileMetadataPtr RemoteFileMetadataFactory::createClass(const String & class_name)
{
    auto it = class_creators.find(class_name);
    if (it == class_creators.end())
        return nullptr;
    return (it->second)();
}

void RemoteFileMetadataFactory::registerClass(const String & class_name, ClassCreator creator)
{
    auto it = class_creators.find(class_name);
    if (it != class_creators.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Class ({}) has been registered. It is a fatal error.", class_name);
    }
    class_creators[class_name] = creator;
}
}
