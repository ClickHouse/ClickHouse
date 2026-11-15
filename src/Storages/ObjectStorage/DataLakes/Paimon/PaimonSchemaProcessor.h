#pragma once
#include <config.h>

#if USE_AVRO

#include <memory>
#include <optional>
#include <unordered_map>
#include <Core/NamesAndTypes.h>
#include <Common/SharedMutex.h>
#include <Common/SharedLockGuard.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>

namespace DB
{

/// Caches parsed schemas and their ClickHouse type mappings.
/// Similar to IcebergSchemaProcessor in design.
class PaimonSchemaProcessor
{
public:
    PaimonSchemaProcessor() = default;

    /// Add a new schema from JSON, returns the parsed schema.
    PaimonTableSchemaPtr addSchema(const Poco::JSON::Object::Ptr & schema_json);

    /// Get existing schema by ID.
    PaimonTableSchemaPtr getSchemaById(Int64 schema_id) const;

    /// Get or add schema - if schema_json is null, only retrieves existing.
    PaimonTableSchemaPtr getOrAddSchema(Int64 schema_id, const Poco::JSON::Object::Ptr & schema_json);

    /// Check if schema exists.
    bool hasSchema(Int64 schema_id) const;

    /// Get ClickHouse column definitions for a schema.
    std::shared_ptr<NamesAndTypesList> getClickHouseSchema(Int64 schema_id);

    /// Register the relationship between snapshot and schema.
    void registerSnapshotSchema(Int64 snapshot_id, Int64 schema_id);

    /// Get the schema_id for a given snapshot.
    std::optional<Int64> getSchemaIdForSnapshot(Int64 snapshot_id) const;

    /// Get partition keys for a schema.
    std::vector<String> getPartitionKeys(Int64 schema_id) const;

    /// Get primary keys for a schema.
    std::vector<String> getPrimaryKeys(Int64 schema_id) const;

    /// Get options for a schema.
    std::unordered_map<String, String> getOptions(Int64 schema_id) const;

private:
    /// Convert PaimonTableSchema to ClickHouse NamesAndTypesList.
    static std::shared_ptr<NamesAndTypesList> convertToClickHouseSchema(const PaimonTableSchemaPtr & schema);

    mutable SharedMutex mutex;

    /// schema_id -> PaimonTableSchema
    std::unordered_map<Int64, PaimonTableSchemaPtr> schemas_by_id TSA_GUARDED_BY(mutex);

    /// schema_id -> ClickHouse NamesAndTypesList (cached conversion result)
    std::unordered_map<Int64, std::shared_ptr<NamesAndTypesList>> clickhouse_schemas_by_id TSA_GUARDED_BY(mutex);

    /// snapshot_id -> schema_id
    std::unordered_map<Int64, Int64> schema_id_by_snapshot TSA_GUARDED_BY(mutex);
};

using PaimonSchemaProcessorPtr = std::shared_ptr<PaimonSchemaProcessor>;

}


#endif

