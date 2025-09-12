#pragma once

#include <optional>
#include <Storages/ObjectStorage/DataLakes/Paimon/Types.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <base/types.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

struct DataField
{
    Int32 id;
    String name;
    Paimon::DataType type;
    std::optional<String> description;

    explicit DataField(const Poco::JSON::Object::Ptr & json_object)
    {
        id = json_object->getValue<decltype(id)>("id");
        name = json_object->getValue<decltype(name)>("name");
        Paimon::getOptionalValueFromJSON(description, json_object, "description");
        type = Paimon::DataType::parse(json_object, "type");
    }
};

struct PaimonTableSchema
{
    Int32 version;
    Int64 id{-1};
    Int32 highest_field_id;
    std::vector<String> partition_keys;
    std::vector<String> primary_keys;
    std::unordered_map<String, String> options;
    std::optional<String> comment;
    Int64 time_mills;
    std::vector<DataField> fields;
    std::unordered_map<String, size_t> fields_by_name_indexes;
    Poco::JSON::Object::Ptr raw_json_object;

    bool operator==(const PaimonTableSchema & other) const { return version == other.version && id == other.id; }

    void update(const Poco::JSON::Object::Ptr & json_object);

    explicit PaimonTableSchema(const Poco::JSON::Object::Ptr & json_object);
};
using PaimonTableSchemaPtr = std::shared_ptr<PaimonTableSchema>;

}
