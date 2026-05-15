#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>

namespace DB
{
PaimonTableSchema::PaimonTableSchema(const Poco::JSON::Object::Ptr & json_object)
    : raw_json_object(json_object)
{
    update(json_object);
}
void PaimonTableSchema::update(const Poco::JSON::Object::Ptr & json_object)
{
    Int32 tmp_id = -1;
    Paimon::getValueFromJSON(tmp_id, json_object, "id");
    /// same schema
    if (tmp_id >= 0 && tmp_id == id)
    {
        return;
    }

    Paimon::getValueFromJSON(version, json_object, "version");
    Paimon::getValueFromJSON(id, json_object, "id");
    Paimon::getValueFromJSON(highest_field_id, json_object, "highestFieldId");
    Paimon::getValueFromJSON(time_mills, json_object, "timeMillis");
    Paimon::getOptionalValueFromJSON(comment, json_object, "comment");

    /// get array
    Paimon::getVecFromJSON(partition_keys, json_object, "partitionKeys");
    Paimon::getVecFromJSON(primary_keys, json_object, "primaryKeys");

    /// get map
    Paimon::getMapFromJSON(options, json_object, "options");

    /// get fields
    const auto & json_array = json_object->getArray("fields");
    fields.reserve(json_array->size());
    for (uint32_t i = 0; i < json_array->size(); ++i)
    {
        fields.emplace_back(json_array->getObject(i));
        fields_by_name_indexes.emplace(fields.back().name, fields.size() - 1);
    }
}
}
