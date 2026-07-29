#pragma once


#include <memory>
#include <mutex>


#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <base/defines.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/SharedMutex.h>

#include <map>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
namespace DB::Iceberg
{

ColumnMapperPtr createColumnMapper(Poco::JSON::Object::Ptr schema_object);

/**
 * Iceberg supports the following data types (see https://iceberg.apache.org/spec/#schemas-and-data-types):
 * - Primitive types:
 *   - boolean
 *   - int
 *   - long
 *   - float
 *   - double
 *   - decimal(P, S)
 *   - date
 *   - time (time of day in microseconds since midnight)
 *   - timestamp (in microseconds since 1970-01-01)
 *   - timestamptz (timestamp with timezone, stores values in UTC timezone)
 *   - timestamp_ns (in nanoseconds since 1970-01-01, format version 3+)
 *   - timestamptz_ns (timestamp with timezone in nanoseconds, format version 3+)
 *   - string
 *   - uuid
 *   - fixed(L) (fixed-length byte array of length L)
 *   - binary
 * - Complex types:
 *   - struct(field1: Type1, field2: Type2, ...) (tuple of typed values)
 *   - list(nested_type)
 *   - map(Key, Value)
 *
 * Example of table schema in metadata:
 * {
 *     "type" : "struct",
 *     "schema-id" : 0,
 *     "fields" : [
 *     {
 *         "id" : 1,
 *         "name" : "id",
 *         "required" : false,
 *         "type" : "long"
 *     },
 *     {
 *         "id" : 2,
 *         "name" : "array",
 *         "required" : false,
 *         "type" : {
 *             "type" : "list",
 *             "element-id" : 5,
 *             "element" : "int",
 *             "element-required" : false
 *     },
 *     {
 *         "id" : 3,
 *         "name" : "data",
 *         "required" : false,
 *         "type" : "binary"
 *     }
 * }
 */
class IcebergSchemaProcessor : private WithContext
{
    static std::string default_link;

    using Node = ActionsDAG::Node;

    /// ClickHouse DateTime64 timezone for timestamptz depends on `iceberg_timezone_for_timestamptz`.
    /// Empty means "use session timezone" (non-explicit DateTime64), distinct from explicit `"UTC"`.
    using SchemaTimezoneKey = std::pair<Int32, String>;
    using FieldTimezoneKey = std::tuple<Int32, String, Int32>; /// schema_id, timezone, source_id
    using TransformDagKey = std::tuple<Int32, Int32, String>; /// old_id, new_id, timezone

public:
    explicit IcebergSchemaProcessor(ContextPtr context_, bool allow_geo_parser_ = false) : WithContext(context_), allow_geo_parser(allow_geo_parser_) {}

    void addIcebergTableSchema(Poco::JSON::Object::Ptr schema_ptr, ContextPtr context_);
    std::shared_ptr<NamesAndTypesList> getClickhouseTableSchemaById(Int32 id, ContextPtr context_);
    std::shared_ptr<const ActionsDAG> getSchemaTransformationDagByIds(ContextPtr context_, Int32 old_id, Int32 new_id);
    NameAndTypePair getFieldCharacteristics(Int32 schema_version, Int32 source_id, ContextPtr context_);
    std::optional<NameAndTypePair> tryGetFieldCharacteristics(Int32 schema_version, Int32 source_id, ContextPtr context_) const;
    NamesAndTypesList tryGetFieldsCharacteristics(Int32 schema_id, const std::vector<Int32> & source_ids, ContextPtr context_) const;
    std::optional<Int32> tryGetColumnIDByName(Int32 schema_id, const std::string & name) const;
    Poco::JSON::Object::Ptr getIcebergTableSchemaById(Int32 id) const;
    bool hasClickhouseTableSchemaById(Int32 id, ContextPtr context_) const;

    static DataTypePtr getSimpleType(const String & type_name, ContextPtr context_, bool allow_geo_parser = true);

    static std::unordered_map<String, Int64> traverseSchema(Poco::JSON::Array::Ptr schema);

    /// Paths whose Iceberg logical type is `string` (not `binary`); both read as DataTypeString.
    static std::unordered_set<String> collectIcebergStringPaths(Poco::JSON::Array::Ptr schema);

    void registerSnapshotWithSchemaId(Int64 snapshot_id, Int32 schema_id);
    Int32 getSchemaIdForSnapshot(Int64 snapshot_id) const;
    std::optional<Int32> tryGetSchemaIdForSnapshot(Int64 snapshot_id) const;

    ColumnMapperPtr getColumnMapperById(Int32 id) const;

private:
    std::unordered_map<Int32, Poco::JSON::Object::Ptr> iceberg_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::map<SchemaTimezoneKey, std::shared_ptr<NamesAndTypesList>> clickhouse_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::map<TransformDagKey, std::shared_ptr<ActionsDAG>> transform_dags_by_ids TSA_GUARDED_BY(mutex);
    mutable std::map<FieldTimezoneKey, NameAndTypePair> clickhouse_types_by_source_ids TSA_GUARDED_BY(mutex);
    mutable std::map<std::pair<Int32, std::string>, Int32> clickhouse_ids_by_source_names TSA_GUARDED_BY(mutex);
    std::optional<Int32> current_schema_id TSA_GUARDED_BY(mutex) = 0;
    std::optional<String> current_materialization_timezone TSA_GUARDED_BY(mutex);
    std::unordered_map<Int64, Int32> schema_id_by_snapshot TSA_GUARDED_BY(mutex);

    NamesAndTypesList getSchemaType(const Poco::JSON::Object::Ptr & schema);
    DataTypePtr getComplexTypeFromObject(
        const Poco::JSON::Object::Ptr & type,
        String & current_full_name,
        ContextPtr context_,
        bool is_subfield_of_root);
    DataTypePtr getFieldType(
        const Poco::JSON::Object::Ptr & field,
        const String & type_key,
        ContextPtr context_,
        bool required,
        String & current_full_name = default_link,
        bool is_subfield_of_root = false);

    bool allowPrimitiveTypeConversion(const String & old_type, const String & new_type);
    const Node * getDefaultNodeForField(const Poco::JSON::Object::Ptr & field);

    std::shared_ptr<ActionsDAG> getSchemaTransformationDag(
        const Poco::JSON::Object::Ptr & old_schema,
        const Poco::JSON::Object::Ptr & new_schema,
        ContextPtr context_,
        Int32 old_id,
        Int32 new_id);

    /// Must be called under exclusive `mutex`. Materializes ClickHouse types for `(schema_id, timezone)`.
    void materializeClickhouseSchemaLocked(Int32 schema_id, Poco::JSON::Object::Ptr schema_ptr, ContextPtr context_) TSA_REQUIRES(mutex);
    /// Must be called under exclusive `mutex`. Drops CH type maps and transform DAGs for `(schema_id, timezone)`.
    void eraseClickhouseSchemaArtifactsLocked(Int32 schema_id, const String & timezone) TSA_REQUIRES(mutex);
    /// Must be called under exclusive `mutex`. Ensures `(schema_id, timezone)` is materialized.
    void ensureClickhouseSchemaMaterializedLocked(Int32 schema_id, ContextPtr context_) TSA_REQUIRES(mutex);

    mutable SharedMutex mutex;
    bool allow_geo_parser = true;
};

using IcebergSchemaProcessorPtr = std::shared_ptr<IcebergSchemaProcessor>;
}
