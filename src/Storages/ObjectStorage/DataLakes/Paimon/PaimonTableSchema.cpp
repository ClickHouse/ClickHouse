#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>

namespace DB
{
PaimonTableSchema::PaimonTableSchema(const Poco::JSON::Object::Ptr & json_object)
:raw_json_object(json_object)
{
    update(json_object);
}
void PaimonTableSchema::update(const Poco::JSON::Object::Ptr & json_object)
{
    Int32 tmp_version = -1;
    Paimon::getValueFromJson(tmp_version, json_object, "version");
    /// same schema
    if (tmp_version >=0 && tmp_version == version)
    {
        return;
    }

    Paimon::getValueFromJson(version, json_object, "version");
    Paimon::getValueFromJson(id, json_object, "id");
    Paimon::getValueFromJson(highest_field_id, json_object, "highestFieldId");
    Paimon::getValueFromJson(time_mills, json_object, "timeMillis");
    Paimon::getValueFromJson(comment, json_object, "comment");
    
    /// get array
    Paimon::getVecFromJson(partition_keys, json_object, "partitionKeys");
    Paimon::getVecFromJson(primary_keys, json_object, "primaryKeys");

    /// get map
    Paimon::getMapFromJson(options, json_object, "options");

    /// get fields
    const auto & json_array = json_object->getArray("fields");
    fields.reserve(json_array->size());
    for (uint32_t i = 0; i < json_array->size(); ++i)
    {
        fields.emplace_back(json_array->getObject(i));
        fields_by_name_indexes.emplace(fields.back().name, fields.size()-1);
    }
}
}
