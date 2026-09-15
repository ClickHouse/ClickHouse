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

#include <unordered_map>
#include <unordered_set>
namespace DB::Iceberg
{

/// Build a ColumnMapper carrying all Iceberg per-path metadata (field ids, string paths, optional
/// paths) from a schema `fields` array. Single wiring point shared by createColumnMapper and the
/// MultipleFileWriter INSERT path so no consumer can drift out of sync.
ColumnMapperPtr createColumnMapperFromFields(Poco::JSON::Array::Ptr fields);

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
class IcebergSchemaProcessor
{
    static std::string default_link;

    using Node = ActionsDAG::Node;

public:
    /// Where a schema copy being registered comes from. metadata.json is the authoritative source;
    /// the 'schema' key of a manifest file header is only a snapshot of the table schema at the time
    /// the manifest was written and loses any conflict with the metadata.json copy of the same
    /// schema-id, whichever of the two was read first.
    enum class SchemaSource
    {
        Metadata,
        ManifestFile,
    };

    explicit IcebergSchemaProcessor(bool allow_geo_parser_ = false) : allow_geo_parser(allow_geo_parser_) {}

    /// `tolerate_conflicting_manifest_schemas` is the value of the setting of the same name in the
    /// operation registering this copy. It governs every conflict this call runs into, including one
    /// between a metadata.json copy registered now and a manifest header copy registered earlier by
    /// another operation, so it has to be passed for metadata.json copies as well.
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

    static DataTypePtr getSimpleType(const String & type_name, bool allow_geo_parser = true);

    static std::unordered_map<String, Int64> traverseSchema(Poco::JSON::Array::Ptr schema);

    /// Paths whose Iceberg logical type is `string` (not `binary`); both read as DataTypeString.
    static std::unordered_set<String> collectIcebergStringPaths(Poco::JSON::Array::Ptr schema);

    /// Paths whose Iceberg field is `optional` (required=false). A complex container is never
    /// Nullable in the ClickHouse type, so a writer emitting Iceberg `required` must consult this.
    static std::unordered_set<String> collectIcebergOptionalPaths(Poco::JSON::Array::Ptr schema);

    void registerSnapshotWithSchemaId(Int64 snapshot_id, Int32 schema_id);
    Int32 getSchemaIdForSnapshot(Int64 snapshot_id) const;
    std::optional<Int32> tryGetSchemaIdForSnapshot(Int64 snapshot_id) const;

    ColumnMapperPtr getColumnMapperById(Int32 id) const;

    void updateLastColumnId(Int32 last_column_id_);

private:
    std::atomic<Int64> last_column_id{-1};

    std::unordered_map<Int32, Poco::JSON::Object::Ptr> iceberg_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::unordered_map<Int32, std::shared_ptr<NamesAndTypesList>> clickhouse_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::map<std::pair<Int32, Int32>, std::shared_ptr<ActionsDAG>> transform_dags_by_ids TSA_GUARDED_BY(mutex);
    mutable std::map<std::pair<Int32, Int32>, NameAndTypePair> clickhouse_types_by_source_ids TSA_GUARDED_BY(mutex);
    mutable std::map<std::pair<Int32, std::string>, Int32> clickhouse_ids_by_source_names TSA_GUARDED_BY(mutex);

    /// The schema currently being converted by `addSchemaImpl`. Its per-field lookups are collected
    /// here and merged into the maps above only after the whole schema has converted, so a malformed
    /// schema that throws halfway leaves nothing behind in the shared processor.
    struct PendingSchema
    {
        Int32 schema_id;
        std::map<std::pair<Int32, Int32>, NameAndTypePair> types_by_source_ids;
        std::map<std::pair<Int32, std::string>, Int32> ids_by_source_names;
    };
    std::optional<PendingSchema> pending_schema TSA_GUARDED_BY(mutex);
    std::unordered_map<Int64, Int32> schema_id_by_snapshot TSA_GUARDED_BY(mutex);

    /// Schema-ids whose registered copy was taken from a manifest file header and has not been
    /// confirmed by metadata.json yet. Only the source is remembered, never the setting of the
    /// operation that registered it: the processor is shared across queries, and each operation
    /// decides under its own `iceberg_tolerate_conflicting_manifest_schemas` value whether a
    /// conflict between such a copy and metadata.json replaces the copy or is an error. The
    /// manifest-walking entrypoints (the `remove_orphan_files` and `expire_snapshots` commands,
    /// mutation validation) reach a manifest file header on a table object whose shared processor
    /// is still empty, so such a copy can be the first registration of its id.
    std::unordered_set<Int32> manifest_only_schema_ids TSA_GUARDED_BY(mutex);

    /// Manifest-only schema-ids whose registered copy was contradicted by another manifest file
    /// header. Neither copy is authoritative, so the id has no usable schema until a metadata.json
    /// copy settles the conflict: every lookup of such an id fails instead of answering with an
    /// arbitrary first header, which could otherwise transform the files of the other manifest with
    /// the wrong schema. A manifest walk that never consults the schema (e.g. collecting file paths)
    /// is unaffected. Always a subset of `manifest_only_schema_ids`.
    std::unordered_set<Int32> unsettled_manifest_schema_ids TSA_GUARDED_BY(mutex);

    /// Throws `ICEBERG_SPECIFICATION_VIOLATION` if `schema_id` is in `unsettled_manifest_schema_ids`.
    void assertSchemaIsSettled(Int32 schema_id) const TSA_REQUIRES_SHARED(mutex);

    /// Registers `schema_ptr` under `schema_id`, first dropping a previously registered copy of the
    /// same id when asked. The schema is fully validated and converted into temporaries before
    /// anything is dropped or written, so a malformed schema leaves the processor exactly as it was:
    /// the previous copy of the id stays registered, and no partial lookups of the bad copy survive
    /// to be picked up by a later registration of the same id.
    void addSchemaImpl(const Poco::JSON::Object::Ptr & schema_ptr, Int32 schema_id, bool replace_existing) TSA_REQUIRES(mutex);

    /// Drops the schema registered for `schema_id` together with everything derived from it.
    void dropSchemaImpl(Int32 schema_id) TSA_REQUIRES(mutex);

    NamesAndTypesList getSchemaType(const Poco::JSON::Object::Ptr & schema);
    DataTypePtr getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type, String & current_full_name, bool is_subfield_of_root);
    DataTypePtr getFieldType(
        const Poco::JSON::Object::Ptr & field,
        const String & type_key,
        bool required,
        String & current_full_name = default_link,
        bool is_subfield_of_root = false);

    bool allowPrimitiveTypeConversion(const String & old_type, const String & new_type);
    const Node * getDefaultNodeForField(const Poco::JSON::Object::Ptr & field);

    std::shared_ptr<ActionsDAG> getSchemaTransformationDag(
        const Poco::JSON::Object::Ptr & old_schema, const Poco::JSON::Object::Ptr & new_schema, Int32 old_id, Int32 new_id);

    mutable SharedMutex mutex;
    bool allow_geo_parser = true;
};

using IcebergSchemaProcessorPtr = std::shared_ptr<IcebergSchemaProcessor>;
}
