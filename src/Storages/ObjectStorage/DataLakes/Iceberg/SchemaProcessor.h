#pragma once


#include <Formats/ColumnNameMaps.h>
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

#include <unordered_map>
#include <unordered_set>
namespace DB::Iceberg
{

/// Build a ColumnMapper carrying all Iceberg per-path metadata (field ids, string paths, optional
/// paths) from a schema `fields` array. Single wiring point shared by createColumnMapper and the
/// MultipleFileWriter INSERT path so no consumer can drift out of sync.
ColumnMapperPtr createColumnMapperFromFields(Poco::JSON::Array::Ptr fields);

ColumnMapperPtr createColumnMapper(Poco::JSON::Object::Ptr schema_object);

/// The `initial-default` of the schema field `field` as a value of its ClickHouse type `type`, or nullopt if the field
/// has none. Throws ICEBERG_SPECIFICATION_VIOLATION if the default is malformed.
std::optional<Field> getIcebergInitialDefault(const Poco::JSON::Object::Ptr & field, const DataTypePtr & type);

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
class IcebergSchemaProcessor
{
    static std::string default_link;

    using Node = ActionsDAG::Node;

public:
    /// Where a schema copy being registered comes from. metadata.json is the authoritative source;
    /// the 'schema' key of a manifest file header is only a snapshot of the table schema at the time
    /// the manifest was written. A schema that came from a manifest is never authoritative: it is
    /// replaced by the metadata.json copy of the same schema-id whenever that one is registered, and
    /// it may be ignored if it conflicts with an already registered metadata.json copy.
    enum class SchemaSource
    {
        Metadata,
        ManifestFile,
    };

    explicit IcebergSchemaProcessor(bool allow_geo_parser_ = false, String timezone_for_timestamptz_ = "UTC")
        : allow_geo_parser(allow_geo_parser_)
        , timezone_for_timestamptz(std::move(timezone_for_timestamptz_))
    {
    }

    void addIcebergTableSchema(
        Poco::JSON::Object::Ptr schema_ptr,
        SchemaSource source = SchemaSource::Metadata,
        bool tolerate_conflicting_manifest_schemas = false);
    std::shared_ptr<NamesAndTypesList> getClickHouseTableSchemaById(Int32 id);
    std::shared_ptr<const ActionsDAG> getSchemaTransformationDagByIds(Int32 old_id, Int32 new_id);
    NameAndTypePair getFieldCharacteristics(Int32 schema_version, Int32 source_id) const;
    std::optional<NameAndTypePair> tryGetFieldCharacteristics(Int32 schema_version, Int32 source_id) const;
    NamesAndTypesList tryGetFieldsCharacteristics(Int32 schema_id, const std::vector<Int32> & source_ids) const;
    std::optional<Int32> tryGetColumnIDByName(Int32 schema_id, const std::string & name) const;
    Poco::JSON::Object::Ptr getIcebergTableSchemaById(Int32 id) const;
    bool hasClickHouseTableSchemaById(Int32 id) const;
    bool isSchemaRegisteredFromMetadata(Int32 id) const;

    static DataTypePtr getSimpleType(const String & type_name, bool allow_geo_parser = true, const String & timezone_for_timestamptz = "UTC");

    /// The first Iceberg spec violation in `schema`, or nullopt: a field, list element, map key or value without the members
    /// its type needs, or invalid geo type parameters. With `is_table_schema`, also a non-`struct` schema or an
    /// `identifier-field-ids` entry that is not a required primitive field outside lists, maps and optional structs.
    static std::optional<String> findSchemaViolation(const Poco::JSON::Object & schema, bool is_table_schema);

    /// Throws ICEBERG_SPECIFICATION_VIOLATION for the first invalid schema of a metadata.json: an entry of `schemas`,
    /// or the legacy `schema` when there is no `schemas`.
    static void checkTableSchemas(const Poco::JSON::Object & metadata);

    static ColumnNameToFieldId traverseSchema(Poco::JSON::Array::Ptr schema);

    /// The largest field id in `schema`, nested ids included.
    static Int64 getMaxFieldId(Poco::JSON::Array::Ptr schema);

    /// Paths whose Iceberg logical type is `string` (not `binary`); both read as DataTypeString.
    static std::unordered_set<String> collectIcebergStringPaths(Poco::JSON::Array::Ptr schema);

    /// Paths whose Iceberg field is `optional` (required=false). A complex container is never
    /// Nullable in the ClickHouse type, so a writer emitting Iceberg `required` must consult this.
    static std::unordered_set<String> collectIcebergOptionalPaths(Poco::JSON::Array::Ptr schema);

    /// Field ids (including nested ones) whose Iceberg type is `time`.
    static std::unordered_set<Int64> collectIcebergTimeFieldIds(Poco::JSON::Array::Ptr schema);

    /// The effective `initial-default` of the fields (including nested ones) that have one, by the paths of `traverseSchema`.
    static std::unordered_map<String, ColumnMapper::IcebergInitialDefault> collectIcebergInitialDefaults(Poco::JSON::Array::Ptr schema);

    void registerSnapshotWithSchemaId(Int64 snapshot_id, Int32 schema_id);
    Int32 getSchemaIdForSnapshot(Int64 snapshot_id) const;
    std::optional<Int32> tryGetSchemaIdForSnapshot(Int64 snapshot_id) const;

    ColumnMapperPtr getColumnMapperById(Int32 id) const;

private:
    std::unordered_map<Int32, Poco::JSON::Object::Ptr> iceberg_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::unordered_map<Int32, std::shared_ptr<NamesAndTypesList>> clickhouse_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::map<std::pair<Int32, Int32>, std::shared_ptr<ActionsDAG>> transform_dags_by_ids TSA_GUARDED_BY(mutex);
    mutable std::map<std::pair<Int32, Int32>, NameAndTypePair> clickhouse_types_by_source_ids TSA_GUARDED_BY(mutex);
    mutable std::map<std::pair<Int32, std::string>, Int32> clickhouse_ids_by_source_names TSA_GUARDED_BY(mutex);
    std::optional<Int32> current_schema_id TSA_GUARDED_BY(mutex) = 0;
    std::unordered_map<Int64, Int32> schema_id_by_snapshot TSA_GUARDED_BY(mutex);
    /// Schema-ids whose registered copy came from a manifest file header and has not been confirmed
    /// by an identical metadata.json copy yet. Such a copy is replaced when metadata.json binds the
    /// schema-id to a different schema, and two manifest headers disagreeing on such a schema-id is
    /// an error, because no authoritative copy is left to decide between them.
    std::unordered_set<Int32> manifest_sourced_schema_ids TSA_GUARDED_BY(mutex);

    /// Forget the schema registered for `schema_id` together with everything derived from it: the
    /// per-field lookups and the cached schema transformation DAGs in either direction. They are
    /// keyed by schema-id and never rebuilt once populated, so a stale entry would keep answering.
    void dropCachedSchema(Int32 schema_id) TSA_REQUIRES(mutex);

    NamesAndTypesList getSchemaType(const Poco::JSON::Object::Ptr & schema);
    DataTypePtr getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type, String & current_full_name, bool is_subfield_of_root);
    DataTypePtr getFieldType(
        const Poco::JSON::Object::Ptr & field,
        const String & type_key,
        bool required,
        String & current_full_name = default_link,
        bool is_subfield_of_root = false);

    bool allowPrimitiveTypeConversion(const String & old_type, const String & new_type);
    /// Returns `new_holder[key]` in which every nested primitive changed by a legal promotion is replaced by the
    /// type at the same place in `old_holder` (struct fields are matched by id). The arguments are not modified.
    Poco::Dynamic::Var getTypeWithOldPrimitives(
        const Poco::JSON::Object::Ptr & old_holder, const Poco::JSON::Object::Ptr & new_holder, const String & key);
    const Node * getDefaultNodeForField(const Poco::JSON::Object::Ptr & field);

    std::shared_ptr<ActionsDAG> getSchemaTransformationDag(
        const Poco::JSON::Object::Ptr & old_schema, const Poco::JSON::Object::Ptr & new_schema, Int32 old_id, Int32 new_id);

    mutable SharedMutex mutex;
    bool allow_geo_parser = true;
    String timezone_for_timestamptz = "UTC";
};

using IcebergSchemaProcessorPtr = std::shared_ptr<IcebergSchemaProcessor>;
}
