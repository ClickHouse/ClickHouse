#include <IO/RemoteFileMetaDataBase.h>
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RemoteFileMetaDataBase::~RemoteFileMetaDataBase() {}

RemoteFileMetaDataFactory & RemoteFileMetaDataFactory::instance()
{
    static RemoteFileMetaDataFactory g_factory;
    return g_factory;
}

RemoteFileMetaDataBasePtr RemoteFileMetaDataFactory::createClass(const String & class_name)
{
    auto it = class_creators.find(class_name);
    if (it == class_creators.end())
        return nullptr;
    return (it->second)();
}

void RemoteFileMetaDataFactory::registerClass(const String & class_name, ClassCreator creator)
{
    auto it = class_creators.find(class_name);
    if (it != class_creators.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Class ({}) has been registered. It is a fatal error.", class_name);
    }
    class_creators[class_name] = creator;
}
}
