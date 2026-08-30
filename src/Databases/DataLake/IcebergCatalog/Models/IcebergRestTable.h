#pragma once
#include "config.h"

#if USE_AVRO

#include <Core/Types.h>
#include <optional>
#include <string>
#include <vector>

namespace Poco
{
namespace JSON
{
    class Object;
}
}

namespace DataLake::IcebergRestModels
{

struct TableIdentifiersPage
{
    std::vector<std::string> tables;
    std::string next_page_token;
};

TableIdentifiersPage parseTableIdentifiersPage(
    const std::string & json,
    const std::string & base_namespace,
    size_t limit = 0);

struct LoadTableResponse
{
    Poco::JSON::Object::Ptr metadata;
    Poco::JSON::Object::Ptr config;
    std::optional<std::string> metadata_location;
    std::optional<std::string> table_uuid;
};

LoadTableResponse parseLoadTableResponse(const std::string & json);

Poco::JSON::Object::Ptr buildCreateTableRequest(
    const std::string & table_name,
    Poco::JSON::Object::Ptr metadata_content,
    bool include_location);

Poco::JSON::Object::Ptr buildUpdateMetadataRequest(
    const std::string & namespace_name,
    const std::string & table_name,
    Poco::JSON::Object::Ptr new_snapshot);

Poco::JSON::Object::Ptr buildUpdateSchemaRequest(
    const std::string & namespace_name,
    const std::string & table_name,
    Poco::JSON::Object::Ptr new_schema,
    Int32 previous_schema_id);

std::string serializeTableIdentifiersPage(const TableIdentifiersPage & page, const std::string & base_namespace);

}

#endif
