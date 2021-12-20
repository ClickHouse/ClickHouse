#pragma once
#include <Storages/IRemoteFileMetadata.h>
#include <memory>
#include <functional>
#include <unordered_map>
namespace DB
{

class RemoteFileMetadataFactory : private boost::noncopyable
{
public:
    using MetadataCreator = std::function<IRemoteFileMetadataPtr()>;
    ~RemoteFileMetadataFactory() = default;

    static RemoteFileMetadataFactory & instance();
    IRemoteFileMetadataPtr get(const String & name);
    void registerRemoteFileMatadataCreator(const String &name, MetadataCreator creator);
protected:
    RemoteFileMetadataFactory() = default;

private:
    std::unordered_map<String, MetadataCreator> class_creators;
};

void registerRemoteFileMatadataCreators();

}
