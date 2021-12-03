#include <Storages/Hive/HiveFileMetaData.h>
#include <Common/Exception.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

HiveFileMetaData::~HiveFileMetaData() = default;

String HiveFileMetaData::toString()
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

bool HiveFileMetaData::fromString(const String &buf)
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

bool HiveFileMetaData::equal(RemoteFileMetaDataBasePtr meta_data)
{
    auto real_meta_data = std::dynamic_pointer_cast<HiveFileMetaData>(meta_data);
    if (!real_meta_data)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid meta data class");
    return last_modification_timestamp == real_meta_data->last_modification_timestamp;
}

REGISTTER_REMOTE_FILE_META_DATA_CLASS(HiveFileMetaData)

}
