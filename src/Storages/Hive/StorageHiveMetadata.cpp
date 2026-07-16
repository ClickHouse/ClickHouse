#include <Storages/Hive/StorageHiveMetadata.h>

#if USE_HIVE

#include <Storages/Cache/RemoteFileMetadataFactory.h>
#include <Common/Exception.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

String StorageHiveMetadata::toString() const
{
    Poco::JSON::Object jobj;
    jobj.set("schema", schema);
    jobj.set("cluster", cluster);
    jobj.set("remote_path", remote_path);
    jobj.set("last_modification_timestamp", last_modification_timestamp);
    jobj.set("file_size", file_size);
    std::stringstream buf; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    jobj.stringify(buf);
    return buf.str();

}

bool StorageHiveMetadata::fromString(const String &buf)
{
    std::stringstream istream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    istream << buf;
    Poco::JSON::Parser parser;
    auto jobj = parser.parse(istream).extract<Poco::JSON::Object::Ptr>();
    remote_path = jobj->get("remote_path").convert<String>();
    schema = jobj->get("schema").convert<String>();
    cluster = jobj->get("cluster").convert<String>();
    last_modification_timestamp = jobj->get("last_modification_timestamp").convert<UInt64>();
    file_size =jobj->get("file_size").convert<UInt64>();
    return true;
}

String StorageHiveMetadata::getVersion() const
{
    return std::to_string(last_modification_timestamp);
}

void registerStorageHiveMetadata(RemoteFileMetadataFactory & factory)
{
    auto creator = []() -> IRemoteFileMetadataPtr { return std::make_shared<StorageHiveMetadata>(); };
    factory.registerRemoteFileMatadata("StorageHiveMetadata", creator);
}

}
#endif

