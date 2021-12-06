#include <IO/IRemoteFileMetadata.h>
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

IRemoteFileMetadataPtr RemoteFileMetadataFactory::get(const String & class_name)
{
    auto it = class_creators.find(class_name);
    if (it == class_creators.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found metadata class:{}", class_name);
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
