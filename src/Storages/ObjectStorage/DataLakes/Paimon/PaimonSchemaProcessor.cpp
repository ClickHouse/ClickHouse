#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonSchemaProcessor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}


PaimonTableSchemaPtr PaimonSchemaProcessor::addSchema(const Poco::JSON::Object::Ptr & schema_json)
{
    if (!schema_json)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot add schema: schema_json is null");

    auto schema = std::make_shared<PaimonTableSchema>(schema_json);
    Int64 schema_id = schema->id;

    std::lock_guard lock(mutex);
    auto [it, inserted] = schemas_by_id.try_emplace(schema_id, schema);
    if (!inserted)
    {
        /// Schema already exists, check if it's the same
        if (it->second->id != schema->id || it->second->version != schema->version)
        {
            it->second = schema;
            clickhouse_schemas_by_id.erase(schema_id);
        }
    }
    return it->second;
}

PaimonTableSchemaPtr PaimonSchemaProcessor::getSchemaById(Int64 schema_id) const
{
    SharedLockGuard lock(mutex);
    auto it = schemas_by_id.find(schema_id);
    if (it == schemas_by_id.end())
        return nullptr;
    return it->second;
}

PaimonTableSchemaPtr PaimonSchemaProcessor::getOrAddSchema(Int64 schema_id, const Poco::JSON::Object::Ptr & schema_json)
{
    /// First try read-only access
    {
        SharedLockGuard lock(mutex);
        auto it = schemas_by_id.find(schema_id);
        if (it != schemas_by_id.end())
            return it->second;
    }

    if (schema_json)
        return addSchema(schema_json);

    return nullptr;
}

bool PaimonSchemaProcessor::hasSchema(Int64 schema_id) const
{
    SharedLockGuard lock(mutex);
    return schemas_by_id.contains(schema_id);
}

std::shared_ptr<NamesAndTypesList> PaimonSchemaProcessor::getClickHouseSchema(Int64 schema_id)
{
    {
        SharedLockGuard lock(mutex);
        auto cache_it = clickhouse_schemas_by_id.find(schema_id);
        if (cache_it != clickhouse_schemas_by_id.end())
            return cache_it->second;

        auto schema_it = schemas_by_id.find(schema_id);
        if (schema_it == schemas_by_id.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema with id {} not found", schema_id);
    }

    /// Need to convert and cache
    std::lock_guard lock(mutex);

    auto cache_it = clickhouse_schemas_by_id.find(schema_id);
    if (cache_it != clickhouse_schemas_by_id.end())
        return cache_it->second;

    auto schema_it = schemas_by_id.find(schema_id);
    if (schema_it == schemas_by_id.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema with id {} not found", schema_id);

    auto ch_schema = convertToClickHouseSchema(schema_it->second);
    clickhouse_schemas_by_id[schema_id] = ch_schema;
    return ch_schema;
}

void PaimonSchemaProcessor::registerSnapshotSchema(Int64 snapshot_id, Int64 schema_id)
{
    std::lock_guard lock(mutex);
    schema_id_by_snapshot[snapshot_id] = schema_id;
}

std::optional<Int64> PaimonSchemaProcessor::getSchemaIdForSnapshot(Int64 snapshot_id) const
{
    SharedLockGuard lock(mutex);
    auto it = schema_id_by_snapshot.find(snapshot_id);
    if (it == schema_id_by_snapshot.end())
        return std::nullopt;
    return it->second;
}

std::vector<String> PaimonSchemaProcessor::getPartitionKeys(Int64 schema_id) const
{
    SharedLockGuard lock(mutex);
    auto it = schemas_by_id.find(schema_id);
    if (it == schemas_by_id.end())
        return {};
    return it->second->partition_keys;
}

std::vector<String> PaimonSchemaProcessor::getPrimaryKeys(Int64 schema_id) const
{
    SharedLockGuard lock(mutex);
    auto it = schemas_by_id.find(schema_id);
    if (it == schemas_by_id.end())
        return {};
    return it->second->primary_keys;
}

std::unordered_map<String, String> PaimonSchemaProcessor::getOptions(Int64 schema_id) const
{
    SharedLockGuard lock(mutex);
    auto it = schemas_by_id.find(schema_id);
    if (it == schemas_by_id.end())
        return {};
    return it->second->options;
}

std::shared_ptr<NamesAndTypesList> PaimonSchemaProcessor::convertToClickHouseSchema(const PaimonTableSchemaPtr & schema)
{
    auto result = std::make_shared<NamesAndTypesList>();
    if (!schema)
        return result;

    for (const auto & field : schema->fields)
    {
        result->emplace_back(field.name, field.type.clickhouse_data_type);
    }
    return result;
}

}


#endif

