#pragma once

#include <memory>
#include <mutex>
#include "config.h"


#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/ActionsDAG.h>
#include <base/defines.h>


#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/SharedMutex.h>

#include <unordered_map>
namespace DB
{

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
    void addIcebergTableSchema(Poco::JSON::Object::Ptr schema_ptr);
    std::shared_ptr<NamesAndTypesList> getClickhouseTableSchemaById(Int32 id);
    std::shared_ptr<const ActionsDAG> getSchemaTransformationDagByIds(Int32 old_id, Int32 new_id);
    NameAndTypePair getFieldCharacteristics(Int32 schema_version, Int32 source_id) const;
    std::optional<NameAndTypePair> tryGetFieldCharacteristics(Int32 schema_version, Int32 source_id) const;
    NamesAndTypesList tryGetFieldsCharacteristics(Int32 schema_id, const std::vector<Int32> & source_ids) const;
    std::optional<Int32> tryGetColumnIDByName(Int32 schema_id, const std::string & name) const;

    bool hasClickhouseTableSchemaById(Int32 id) const;

    static DataTypePtr getSimpleType(const String & type_name);
private:
    std::unordered_map<Int32, Poco::JSON::Object::Ptr> iceberg_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::unordered_map<Int32, std::shared_ptr<NamesAndTypesList>> clickhouse_table_schemas_by_ids TSA_GUARDED_BY(mutex);
    std::map<std::pair<Int32, Int32>, std::shared_ptr<ActionsDAG>> transform_dags_by_ids TSA_GUARDED_BY(mutex);
    mutable std::map<std::pair<Int32, Int32>, NameAndTypePair> clickhouse_types_by_source_ids TSA_GUARDED_BY(mutex);
    mutable std::map<std::pair<Int32, std::string>, Int32> clickhouse_ids_by_source_names TSA_GUARDED_BY(mutex);
    std::optional<Int32> current_schema_id TSA_GUARDED_BY(mutex) = 0;

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
};

}
