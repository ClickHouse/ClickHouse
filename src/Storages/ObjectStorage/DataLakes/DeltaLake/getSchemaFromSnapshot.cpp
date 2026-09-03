#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <DataTypes/DataTypeFactory.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaTypeMapping.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>

#include <base/scope_guard.h>
#include <Core/TypeId.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <string_view>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>

#include <IO/WriteHelpers.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/Dynamic/Var.h>

#include <fmt/format.h>

#include "delta_kernel_ffi.hpp"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    DataTypePtr getSimpleDataTypeFromTypeIndex(TypeIndex type_index)
    {
        std::string_view name = magic_enum::enum_name(type_index);
        return DB::DataTypeFactory::instance().get(std::string(name), nullptr);
    }

    bool isSimpleDataType(TypeIndex type_index)
    {
        switch (type_index)
        {
            case TypeIndex::UInt8: [[fallthrough]];
            case TypeIndex::UInt16: [[fallthrough]];
            case TypeIndex::UInt32: [[fallthrough]];
            case TypeIndex::UInt64: [[fallthrough]];
            case TypeIndex::UInt128: [[fallthrough]];
            case TypeIndex::UInt256: [[fallthrough]];
            case TypeIndex::Int8: [[fallthrough]];
            case TypeIndex::Int16: [[fallthrough]];
            case TypeIndex::Int32: [[fallthrough]];
            case TypeIndex::Int64: [[fallthrough]];
            case TypeIndex::Int128: [[fallthrough]];
            case TypeIndex::Int256: [[fallthrough]];
            case TypeIndex::Float32: [[fallthrough]];
            case TypeIndex::Float64: [[fallthrough]];
            case TypeIndex::Date: [[fallthrough]];
            case TypeIndex::Date32: [[fallthrough]];
            case TypeIndex::DateTime: [[fallthrough]];
            case TypeIndex::DateTime64: [[fallthrough]];
            case TypeIndex::UUID: [[fallthrough]];
            case TypeIndex::String:
                return true;
            default:
                return false;
        }
    }
}
}

namespace DeltaLake
{

/**
 * A helper class for SchemaVisitor.
 * Holds state for SchemaVisitor methods to collect visitor result.
 */
class SchemaVisitorData
{
    friend class SchemaVisitor;

public:
    /// `engine` is required by FFI helpers such as `ffi::get_from_string_map`. Pass `nullptr` only
    /// for visit paths that make no engine-bound FFI calls (e.g. partition column extraction).
    explicit SchemaVisitorData(ffi::SharedExternEngine * engine_) : engine(engine_) {}

    struct SchemaResult
    {
        DB::NamesAndTypesList names_and_types;
        DB::NameToNameMap physical_names_map;
    };
    SchemaResult getSchemaResult();
    const DB::Names & getPartitionColumns() const { return partition_columns; }

private:
    struct Field;
    DB::NamesAndTypesList getNamesAndTypesFromList(
        size_t list_idx,
        const std::string & parent_logical_path,
        const std::string & parent_physical_path,
        DB::NameToNameMap & physical_names_map);

    struct Field
    {
        Field(
            const std::string & name_,
            const DB::TypeIndex & type_,
            bool nullable_,
            const std::string & physical_name_)
            : name(name_), type(type_), nullable(nullable_), physical_name(physical_name_) {}

        /// Column name.
        const std::string name;
        /// Column type.
        const DB::TypeIndex type;
        /// Column nullability.
        const bool nullable;
        /// In case of columnMapping.mode = 'name',
        /// physical name of the column in parquet metadata
        /// will be different from table schema column name.
        const std::string physical_name;

        /// If type is complex (array, map, struct), whether it can contain nullable values.
        bool value_contains_null{};
        /// If type is complex (array, map, struct), list id of the child list.
        size_t child_list_id{};

        size_t precision = 0; /// For Decimal.
        size_t scale = 0; /// For Decimal.

        /// There is no TypeIndex::Bool, so we need to tell
        /// when it is int8 and when it is bool.
        bool is_bool = false;
    };
    using Fields = std::vector<Field>;

    /// See delta-kernel-rs/fii/src/schema.rs for type_lists explanation.
    std::unordered_map<size_t, std::unique_ptr<Fields>> type_lists;
    /// Global counter for type lists.
    size_t list_counter = 0;
    /// A list of partition columns.
    /// Partition columns are not shown in global read schema,
    /// because they are not stored in the actual data,
    /// but instead in data paths directories.
    DB::Names partition_columns;
    /// Engine handle required by v0.23.0 FFI helpers such as `ffi::get_from_string_map`.
    ffi::SharedExternEngine * engine;

    std::exception_ptr visitor_exception;

    const LoggerPtr log = getLogger("SchemaVisitor");

    using KernelScan = KernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
};

/**
 * A schema visitor class.
 * To get table schema, call visitTableSchema().
 * To get read schema, call visitReadSchema().
 * To get partition columns, call visitPartitionColumns().
 */
class SchemaVisitor
{
    using KernelSharedSchema = KernelPointerWrapper<ffi::SharedSchema, ffi::free_schema>;
    using KernelStringSliceIterator = KernelPointerWrapper<ffi::StringSliceIterator, ffi::free_string_slice_data>;
public:
    static void visitTableSchema(ffi::SharedSnapshot * snapshot, SchemaVisitorData & data)
    {
        KernelSharedSchema schema(ffi::logical_schema(snapshot));
        auto visitor = createVisitor(data);
        [[maybe_unused]] size_t result = ffi::visit_schema(schema.get(), &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        if (data.visitor_exception)
            std::rethrow_exception(data.visitor_exception);
    }

    static void visitReadSchema(ffi::SharedScan * scan, SchemaVisitorData & data)
    {
        KernelSharedSchema schema(ffi::scan_physical_schema(scan));
        auto visitor = createVisitor(data);
        [[maybe_unused]] size_t result = ffi::visit_schema(schema.get(), &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        if (data.visitor_exception)
            std::rethrow_exception(data.visitor_exception);
    }

    static void visitWriteSchema(ffi::SharedWriteContext * write_context, SchemaVisitorData & data)
    {
        KernelSharedSchema schema(ffi::get_write_schema(write_context));
        auto visitor = createVisitor(data);
        [[maybe_unused]] size_t result = ffi::visit_schema(schema.get(), &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        if (data.visitor_exception)
            std::rethrow_exception(data.visitor_exception);
    }

    static void visitPartitionColumns(ffi::SharedSnapshot * snapshot, SchemaVisitorData & data)
    {
        KernelStringSliceIterator partition_columns_iter(ffi::get_partition_columns(snapshot));
        while (ffi::string_slice_next(partition_columns_iter.get(), &data, &visitorWrapper<visitPartitionColumn>)) {}

        if (data.visitor_exception)
            std::rethrow_exception(data.visitor_exception);
    }

    static void visitSchema(ffi::SharedSchema * schema, SchemaVisitorData & data)
    {
        auto visitor = createVisitor(data);
        [[maybe_unused]] size_t result = ffi::visit_schema(schema, &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        if (data.visitor_exception)
            std::rethrow_exception(data.visitor_exception);
    }

private:
    static void setVisitorException(SchemaVisitorData * state)
    {
        if (!state->visitor_exception)
            state->visitor_exception = std::current_exception();
    }

    template <auto Func, typename... Args>
    static std::invoke_result_t<decltype(Func), void*, Args...> visitorWrapper(void * data, Args... args)
    {
        SchemaVisitorData * state = static_cast<SchemaVisitorData *>(data);
        if (!state->visitor_exception)
        {
            try
            {
                return Func(data, args...);
            }
            catch (...)
            {
                LOG_ERROR(state->log, "Error while visiting schema: {}", DB::getCurrentExceptionMessage(true));
                setVisitorException(state);
            }
        }
        if constexpr (std::is_void_v<decltype(Func(data, args...))>)
            return;
        else
            return {};
    }

    static void visitPartitionColumn(void * data, ffi::KernelStringSlice slice)
    {
        SchemaVisitorData * state = static_cast<SchemaVisitorData *>(data);
        state->partition_columns.push_back(KernelUtils::fromDeltaString(slice));
    }

    static ffi::EngineSchemaVisitor createVisitor(SchemaVisitorData & data)
    {
        return ffi::EngineSchemaVisitor{
            .data = &data,
            .make_field_list = &visitorWrapper<makeFieldList>,
            .visit_struct = &visitorWrapper<tupleTypeVisitor>,
            .visit_array = &visitorWrapper<arrayTypeVisitor>,
            .visit_map = &visitorWrapper<mapTypeVisitor>,
            .visit_decimal = &visitorWrapper<decimalTypeVisitor>,
            .visit_string = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::String>>,
            .visit_long = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Int64>>,
            .visit_integer = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Int32>>,
            .visit_short = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Int16>>,
            .visit_byte = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Int8>>,
            .visit_float = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Float32>>,
            .visit_double = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Float64>>,
            .visit_boolean = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Int8, true>>,
            .visit_binary = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::String>>,
            .visit_date = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::Date32>>,
            .visit_timestamp = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::DateTime64>>,
            .visit_timestamp_ntz = &visitorWrapper<simpleTypeVisitor<DB::TypeIndex::DateTime64>>,
            .visit_variant = &visitorWrapper<visitVariant>,
        };
    }

    static uintptr_t makeFieldList(void * data, uintptr_t capacity_hint)
    {
        SchemaVisitorData * state = static_cast<SchemaVisitorData *>(data);
        size_t id = state->list_counter++;

        auto list = std::make_unique<SchemaVisitorData::Fields>();
        if (capacity_hint > 0)
            list->reserve(capacity_hint);

        state->type_lists.emplace(id, std::move(list));
        return id;
    }

    static std::unique_ptr<std::string> extractPhysicalName(
        const ffi::CStringMap * metadata,
        SchemaVisitorData * state)
    {
        std::string * physical_name = static_cast<std::string *>(KernelUtils::unwrapResult(
            ffi::get_from_string_map(
                metadata,
                KernelUtils::toDeltaString("delta.columnMapping.physicalName"),
                KernelUtils::allocateString,
                state->engine),
            "get_from_string_map"));
        return physical_name ? std::unique_ptr<std::string>(physical_name) : nullptr;
    }

    template <DB::TypeIndex type, bool is_bool = false>
    static void simpleTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool nullable,
        const ffi::CStringMap * metadata)
    {
        SchemaVisitorData * state = static_cast<SchemaVisitorData *>(data);
        auto it = state->type_lists.find(sibling_list_id);
        if (it == state->type_lists.end())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "List with id {} does not exist", sibling_list_id);
        }

        const std::string column_name(name.ptr, name.len);
        const auto physical_name_ptr = extractPhysicalName(metadata, state);
        const std::string physical_name = physical_name_ptr ? *physical_name_ptr : "";

        LOG_TEST(
            state->log,
            "List id: {}, column name: {} (physical name: {}), type: {}, nullable: {}",
            sibling_list_id, column_name, physical_name, type, nullable);

        SchemaVisitorData::Field field(column_name, std::move(type), nullable, physical_name);
        field.is_bool = is_bool;
        it->second->push_back(std::move(field));
    }

    static void decimalTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool nullable,
        const ffi::CStringMap * metadata,
        uint8_t precision,
        uint8_t scale)
    {
        const auto type = DB::TypeIndex::Decimal32;
        SchemaVisitorData * state = static_cast<SchemaVisitorData *>(data);
        auto it = state->type_lists.find(sibling_list_id);
        if (it == state->type_lists.end())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "List with id {} does not exist", sibling_list_id);
        }

        const std::string column_name(name.ptr, name.len);
        const auto physical_name_ptr = extractPhysicalName(metadata, state);
        const std::string physical_name = physical_name_ptr ? *physical_name_ptr : "";

        LOG_TEST(
            state->log,
            "List id: {}, column name: {} (physical name: {}), type: {}, nullable: {}",
            sibling_list_id, column_name, physical_name, type, nullable);

        SchemaVisitorData::Field field(column_name, type, nullable, physical_name);
        field.precision = precision;
        field.scale = scale;
        it->second->push_back(std::move(field));
    }

    static void arrayTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool nullable,
        const ffi::CStringMap * metadata,
        uintptr_t child_list_id)
    {
        listBasedTypeVisitor<DB::TypeIndex::Array>(data, sibling_list_id, name, nullable, metadata, child_list_id);
    }

    static void tupleTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool nullable,
        const ffi::CStringMap * metadata,
        uintptr_t child_list_id)
    {
        listBasedTypeVisitor<DB::TypeIndex::Tuple>(data, sibling_list_id, name, nullable, metadata, child_list_id);
    }

    static void mapTypeVisitor(
        void *data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool nullable,
        const ffi::CStringMap * metadata,
        uintptr_t child_list_id)
    {
        listBasedTypeVisitor<DB::TypeIndex::Map>(data, sibling_list_id, name, nullable, metadata, child_list_id);
    }

    static void visitVariant(
        [[maybe_unused]] void * data,
        [[maybe_unused]] uintptr_t sibling_list_id,
        [[maybe_unused]] ffi::KernelStringSlice name,
        [[maybe_unused]] bool nullable,
        [[maybe_unused]] const ffi::CStringMap * metadata)
    {
        /// Not simple to support,
        /// delta lake has its own Variant serialization.
        const std::string column_name(name.ptr, name.len);
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Unsupported Variant data type: {}", column_name);
    }

    template <DB::TypeIndex type>
    static void listBasedTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool nullable,
        const ffi::CStringMap * metadata,
        uintptr_t child_list_id)
    {
        SchemaVisitorData * state = static_cast<SchemaVisitorData *>(data);
        auto it = state->type_lists.find(sibling_list_id);
        if (it == state->type_lists.end())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "List with id {} does not exist", sibling_list_id);
        }

        const std::string column_name(name.ptr, name.len);
        const auto physical_name_ptr = extractPhysicalName(metadata, state);
        const std::string physical_name = physical_name_ptr ? *physical_name_ptr : "";

        LOG_TEST(
            state->log,
            "List id: {}, column name: {} (physical name: {}), type: {}, "
            "nullable: {}, child list id: {}",
            sibling_list_id, column_name, physical_name, type, nullable, child_list_id);

        SchemaVisitorData::Field field(column_name, std::move(type), nullable, physical_name);
        field.child_list_id = child_list_id;
        it->second->push_back(field);
    }
};

SchemaVisitorData::SchemaResult SchemaVisitorData::getSchemaResult()
{
    SchemaResult result;
    result.names_and_types = getNamesAndTypesFromList(0, "", "", result.physical_names_map);
    chassert(result.names_and_types.size() == type_lists[0]->size());
    return result;
}

DB::NamesAndTypesList SchemaVisitorData::getNamesAndTypesFromList(
    size_t list_idx,
    const std::string & parent_logical_path,
    const std::string & parent_physical_path,
    DB::NameToNameMap & physical_names_map)
{
    DB::NamesAndTypesList names_and_types;
    for (const auto & field : *type_lists[list_idx])
    {
        DB::DataTypePtr type;
        if (field.is_bool)
        {
            type = DB::DataTypeFactory::instance().get("Bool");
            if (field.nullable)
                type = std::make_shared<DB::DataTypeNullable>(type);
        }
        else if (field.type == DB::TypeIndex::Decimal32)
        {
            type = DB::createDecimal<DB::DataTypeDecimal>(field.precision, field.scale);
            if (field.nullable)
                type = std::make_shared<DB::DataTypeNullable>(type);
        }
        else if (field.type == DB::TypeIndex::DateTime64)
        {
            type = std::make_shared<DB::DataTypeDateTime64>(6);
            if (field.nullable)
                type = std::make_shared<DB::DataTypeNullable>(type);
        }
        else if (DB::isSimpleDataType(field.type))
        {
            type = DB::getSimpleDataTypeFromTypeIndex(field.type);
            if (field.nullable)
                type = std::make_shared<DB::DataTypeNullable>(type);
        }
        else
        {
            if (!field.child_list_id)
            {
                throw DB::Exception(
                    DB::ErrorCodes::NOT_IMPLEMENTED,
                    "Unsupported simple data type: {}", field.type);
            }

            DB::WhichDataType which(field.type);
            /// Compute full ancestor paths for this field so children at any
            /// depth use the complete logical/physical path as the map key/value.
            const std::string field_logical_path = parent_logical_path.empty()
                ? field.name
                : parent_logical_path + "." + field.name;
            const std::string field_physical_path = (!field.physical_name.empty() && !parent_physical_path.empty())
                ? parent_physical_path + "." + field.physical_name
                : field.physical_name;

            if (which.isTuple())
            {
                auto child_names_and_types = getNamesAndTypesFromList(field.child_list_id, field_logical_path, field_physical_path, physical_names_map);
                type = std::make_shared<DB::DataTypeTuple>(child_names_and_types.getTypes(), child_names_and_types.getNames());
            }
            else if (which.isArray())
            {
                auto child_types = getNamesAndTypesFromList(field.child_list_id, field_logical_path, field_physical_path, physical_names_map);
                if (child_types.size() != 1)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Unexpected number of types in array: {}",
                        child_types.size());
                }

                type = std::make_shared<DB::DataTypeArray>(child_types.getTypes()[0]);
            }
            else if (which.isMap())
            {
                auto child_names_and_types = getNamesAndTypesFromList(field.child_list_id, field_logical_path, field_physical_path, physical_names_map);
                auto child_types = child_names_and_types.getTypes();
                if (child_types.size() != 2)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Unexpected number of types in array: {}",
                        child_types.size());
                }
                type = std::make_shared<DB::DataTypeMap>(child_types[0], child_types[1]);
            }
            else
            {
                throw DB::Exception(
                    DB::ErrorCodes::NOT_IMPLEMENTED,
                    "Column {} has unsupported complex data type: {}", field.name, field.type);
            }
        }
        chassert(type);
        if (!field.physical_name.empty())
        {
            /// Use the full ancestor path as the map key so that lookups in
            /// replaceTypeNamesToPhysicalRecursively work at any nesting depth.
            /// key:   "grandparent.parent.field"  (full logical path)
            /// value: "grandparent_phys.parent_phys.field_phys" (full physical path)
            const std::string logical_path = parent_logical_path.empty()
                ? field.name
                : parent_logical_path + "." + field.name;
            const std::string physical_path = parent_physical_path.empty()
                ? field.physical_name
                : parent_physical_path + "." + field.physical_name;
            physical_names_map.emplace(logical_path, physical_path);
        }
        names_and_types.emplace_back(field.name, type);
    }
    return names_and_types;
}

std::pair<DB::NamesAndTypesList, DB::NameToNameMap> getTableSchemaFromSnapshot(
    ffi::SharedSnapshot * snapshot, ffi::SharedExternEngine * engine)
{
    SchemaVisitorData data(engine);
    SchemaVisitor::visitTableSchema(snapshot, data);
    auto result = data.getSchemaResult();
    return {result.names_and_types, result.physical_names_map};
}

DB::NamesAndTypesList getReadSchemaFromSnapshot(ffi::SharedScan * scan, ffi::SharedExternEngine * engine)
{
    SchemaVisitorData data(engine);
    SchemaVisitor::visitReadSchema(scan, data);
    return data.getSchemaResult().names_and_types;
}

DB::NamesAndTypesList getWriteSchema(ffi::SharedWriteContext * write_context, ffi::SharedExternEngine * engine)
{
    SchemaVisitorData data(engine);
    SchemaVisitor::visitWriteSchema(write_context, data);
    return data.getSchemaResult().names_and_types;
}

DB::Names getPartitionColumnsFromSnapshot(ffi::SharedSnapshot * snapshot)
{
    /// Partition column extraction makes no engine-bound FFI calls, so no engine is needed.
    SchemaVisitorData data(nullptr);
    SchemaVisitor::visitPartitionColumns(snapshot, data);
    return data.getPartitionColumns();
}

DB::NamesAndTypesList convertToClickHouseSchema(ffi::SharedSchema * schema, ffi::SharedExternEngine * engine)
{
    SchemaVisitorData data(engine);
    SchemaVisitor::visitSchema(schema, data);
    return data.getSchemaResult().names_and_types;
}

namespace
{

/// Visitor that walks a kernel `SharedSchema` and builds the Delta `StructType.fields` JSON, preserving the
/// exact Delta types. Unlike `SchemaVisitor`, it does not collapse `binary`->`String` or
/// `timestamp_ntz`->`DateTime64`, so a schema round-tripped through it stays byte-identical to the `_delta_log`.
struct DeltaJSONSchemaVisitor
{
    struct Node
    {
        enum class Kind { Primitive, Array, Map, Struct };
        std::string name;
        bool nullable = false;
        Kind kind = Kind::Primitive;
        /// Delta type string for a primitive (e.g. "binary", "timestamp_ntz", "decimal(10,2)").
        std::string primitive;
        /// Child field list id for array/map/struct.
        size_t child_list_id = 0;
    };

    std::unordered_map<size_t, std::vector<Node>> lists;
    /// List id 0 is reserved by the kernel FFI for "None", so allocate ids starting from 1.
    size_t counter = 1;
    std::exception_ptr exception;
    bool has_char_varchar = false;

    const std::vector<Node> & listAt(size_t id) const
    {
        auto it = lists.find(id);
        if (it == lists.end())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Delta schema list with id {} does not exist", id);
        return it->second;
    }

    std::vector<Node> & listAt(size_t id)
    {
        auto it = lists.find(id);
        if (it == lists.end())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Delta schema list with id {} does not exist", id);
        return it->second;
    }

    Poco::Dynamic::Var buildType(const Node & node) const
    {
        checkStackSize();
        switch (node.kind)
        {
            case Node::Kind::Primitive:
                return node.primitive;
            case Node::Kind::Array:
            {
                const auto & children = listAt(node.child_list_id);
                Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;
                obj->set("type", "array");
                obj->set("elementType", buildType(children.at(0)));
                obj->set("containsNull", children.at(0).nullable);
                return obj;
            }
            case Node::Kind::Map:
            {
                const auto & children = listAt(node.child_list_id);
                Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;
                obj->set("type", "map");
                obj->set("keyType", buildType(children.at(0)));
                obj->set("valueType", buildType(children.at(1)));
                obj->set("valueContainsNull", children.at(1).nullable);
                return obj;
            }
            case Node::Kind::Struct:
            {
                Poco::JSON::Object::Ptr obj = new Poco::JSON::Object;
                obj->set("type", "struct");
                obj->set("fields", buildFields(node.child_list_id));
                return obj;
            }
        }
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unhandled Delta schema node kind");
    }

    Poco::JSON::Array::Ptr buildFields(size_t list_id) const
    {
        Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
        for (const auto & node : listAt(list_id))
        {
            Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
            field->set("name", node.name);
            field->set("type", buildType(node));
            field->set("nullable", node.nullable);
            field->set("metadata", Poco::JSON::Object::Ptr(new Poco::JSON::Object));
            fields->add(field);
        }
        return fields;
    }
};

/// Run a visitor mutation, capturing any exception so it never unwinds through the Rust FFI frames.
template <typename Func>
void deltaVisitGuarded(void * data, Func && func)
{
    auto * v = static_cast<DeltaJSONSchemaVisitor *>(data);
    if (v->exception)
        return;
    try
    {
        func(*v);
    }
    catch (...)
    {
        v->exception = std::current_exception();
    }
}

uintptr_t deltaMakeFieldList(void * data, uintptr_t reserve)
{
    auto * v = static_cast<DeltaJSONSchemaVisitor *>(data);
    if (v->exception)
        return 0;
    try
    {
        size_t id = v->counter++;
        auto & list = v->lists[id];
        if (reserve > 0)
            list.reserve(reserve);
        return id;
    }
    catch (...)
    {
        v->exception = std::current_exception();
        return 0;
    }
}

/// True if the field metadata carries the Spark `__CHAR_VARCHAR_TYPE_STRING` annotation (a CHAR(n)/VARCHAR(n)
/// column stored physically as `string`).
bool fieldIsCharVarchar(const ffi::CStringMap * metadata)
{
    if (!metadata)
        return false;
    bool found = false;
    ffi::visit_string_map(
        metadata, &found,
        [](ffi::NullableCvoid ctx, ffi::KernelStringSlice key, ffi::KernelStringSlice)
        {
            if (std::string_view(key.ptr, key.len) == "__CHAR_VARCHAR_TYPE_STRING")
                *static_cast<bool *>(ctx) = true;
        });
    return found;
}

void deltaPushPrimitive(void * data, uintptr_t sibling, ffi::KernelStringSlice name, bool nullable, std::string type)
{
    deltaVisitGuarded(data, [&](DeltaJSONSchemaVisitor & v)
    {
        DeltaJSONSchemaVisitor::Node node;
        node.name.assign(name.ptr, name.len);
        node.nullable = nullable;
        node.kind = DeltaJSONSchemaVisitor::Node::Kind::Primitive;
        node.primitive = std::move(type);
        v.listAt(sibling).push_back(std::move(node));
    });
}

void deltaPushComplex(void * data, uintptr_t sibling, ffi::KernelStringSlice name, bool nullable, uintptr_t child_list_id, DeltaJSONSchemaVisitor::Node::Kind kind)
{
    deltaVisitGuarded(data, [&](DeltaJSONSchemaVisitor & v)
    {
        DeltaJSONSchemaVisitor::Node node;
        node.name.assign(name.ptr, name.len);
        node.nullable = nullable;
        node.kind = kind;
        node.child_list_id = child_list_id;
        v.listAt(sibling).push_back(std::move(node));
    });
}

void deltaVisitBoolean(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "boolean"); }
void deltaVisitByte(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "byte"); }
void deltaVisitShort(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "short"); }
void deltaVisitInteger(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "integer"); }
void deltaVisitLong(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "long"); }
void deltaVisitFloat(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "float"); }
void deltaVisitDouble(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "double"); }
void deltaVisitString(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap * metadata)
{
    deltaVisitGuarded(d, [&](DeltaJSONSchemaVisitor & v)
    {
        if (fieldIsCharVarchar(metadata))
            v.has_char_varchar = true;
    });
    deltaPushPrimitive(d, s, n, nu, "string");
}
void deltaVisitBinary(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "binary"); }
void deltaVisitDate(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "date"); }
void deltaVisitTimestamp(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "timestamp"); }
void deltaVisitTimestampNtz(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *) { deltaPushPrimitive(d, s, n, nu, "timestamp_ntz"); }

void deltaVisitDecimal(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *, uint8_t precision, uint8_t scale)
{
    deltaPushPrimitive(d, s, n, nu, fmt::format("decimal({},{})", precision, scale));
}

void deltaVisitVariant(void * d, uintptr_t, ffi::KernelStringSlice n, bool, const ffi::CStringMap *)
{
    deltaVisitGuarded(d, [&](DeltaJSONSchemaVisitor &)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Unsupported Variant data type: {}", std::string(n.ptr, n.len));
    });
}

void deltaVisitStruct(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *, uintptr_t c) { deltaPushComplex(d, s, n, nu, c, DeltaJSONSchemaVisitor::Node::Kind::Struct); }
void deltaVisitArray(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *, uintptr_t c) { deltaPushComplex(d, s, n, nu, c, DeltaJSONSchemaVisitor::Node::Kind::Array); }
void deltaVisitMap(void * d, uintptr_t s, ffi::KernelStringSlice n, bool nu, const ffi::CStringMap *, uintptr_t c) { deltaPushComplex(d, s, n, nu, c, DeltaJSONSchemaVisitor::Node::Kind::Map); }

}

Poco::JSON::Array::Ptr getDeltaSchemaFieldsFromSnapshot(ffi::SharedSnapshot * snapshot)
{
    using KernelSharedSchema = KernelPointerWrapper<ffi::SharedSchema, ffi::free_schema>;
    KernelSharedSchema schema(ffi::logical_schema(snapshot));

    DeltaJSONSchemaVisitor visitor;
    ffi::EngineSchemaVisitor ffi_visitor{
        .data = &visitor,
        .make_field_list = &deltaMakeFieldList,
        .visit_struct = &deltaVisitStruct,
        .visit_array = &deltaVisitArray,
        .visit_map = &deltaVisitMap,
        .visit_decimal = &deltaVisitDecimal,
        .visit_string = &deltaVisitString,
        .visit_long = &deltaVisitLong,
        .visit_integer = &deltaVisitInteger,
        .visit_short = &deltaVisitShort,
        .visit_byte = &deltaVisitByte,
        .visit_float = &deltaVisitFloat,
        .visit_double = &deltaVisitDouble,
        .visit_boolean = &deltaVisitBoolean,
        .visit_binary = &deltaVisitBinary,
        .visit_date = &deltaVisitDate,
        .visit_timestamp = &deltaVisitTimestamp,
        .visit_timestamp_ntz = &deltaVisitTimestampNtz,
        .visit_variant = &deltaVisitVariant,
    };

    uintptr_t top_level_list_id = ffi::visit_schema(schema.get(), &ffi_visitor);
    if (visitor.exception)
        std::rethrow_exception(visitor.exception);

    /// CHAR(n)/VARCHAR(n) columns are stored as `string` with a field-metadata annotation the raw-schema
    /// helper drops, so the registered catalog schema would differ from the `_delta_log`. Reject onboarding
    /// such tables (mirrors the column-mapping rejection).
    if (visitor.has_char_varchar)
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Registering a DeltaLake table with CHAR/VARCHAR columns into a catalog is not supported "
            "(the char/varchar annotation cannot be preserved in the catalog schema)");

    return visitor.buildFields(top_level_list_id);
}

/// CH -> kernel schema visitor for `ffi::get_create_table_builder`: registers each leaf via `ffi::visit_field_*`, then the top-level struct.

namespace
{

/// Register one ClickHouse column as a Delta field; nested types recurse (array elements and map
/// keys/values are registered as anonymous fields with an empty name).
uintptr_t visitFieldFromClickHouseType(
    ffi::KernelSchemaVisitorState * state,
    const std::string & name,
    const DB::DataTypePtr & full_type)
{
    bool nullable = full_type->isNullable();
    DB::DataTypePtr type = nullable ? DB::removeNullable(full_type) : full_type;
    auto name_slice = KernelUtils::toDeltaString(name);

    auto unwrap = [&](auto result, const char * label)
    {
        return KernelUtils::unwrapResult(result, label);
    };

    /// Nested types recurse to register children; leaf types are classified (and rejected if not round-tripping) via `classifyDeltaPrimitive`.
    switch (type->getTypeId())
    {
        case DB::TypeIndex::Array:
        {
            const auto & array_type = assert_cast<const DB::DataTypeArray &>(*type);
            auto element_id = visitFieldFromClickHouseType(state, /* name */ "", array_type.getNestedType());
            return unwrap(
                ffi::visit_field_array(state, name_slice, element_id, nullable, &KernelUtils::allocateError),
                "visit_field_array");
        }
        case DB::TypeIndex::Map:
        {
            const auto & map_type = assert_cast<const DB::DataTypeMap &>(*type);
            auto key_id = visitFieldFromClickHouseType(state, /* name */ "", map_type.getKeyType());
            auto value_id = visitFieldFromClickHouseType(state, /* name */ "", map_type.getValueType());
            return unwrap(
                ffi::visit_field_map(state, name_slice, key_id, value_id, nullable, &KernelUtils::allocateError),
                "visit_field_map");
        }
        case DB::TypeIndex::Tuple:
        {
            const auto & tuple_type = assert_cast<const DB::DataTypeTuple &>(*type);
            const auto & element_types = tuple_type.getElements();
            const auto & element_names = tuple_type.getElementNames();
            std::vector<uintptr_t> child_ids;
            child_ids.reserve(element_types.size());
            for (size_t i = 0; i < element_types.size(); ++i)
                child_ids.push_back(visitFieldFromClickHouseType(state, element_names[i], element_types[i]));
            return unwrap(
                ffi::visit_field_struct(state, name_slice, child_ids.data(), child_ids.size(), nullable, &KernelUtils::allocateError),
                "visit_field_struct");
        }
        default:
            break;
    }

    switch (DB::classifyDeltaPrimitive(type))
    {
        case DB::DeltaPrimitiveType::Boolean:
            return unwrap(ffi::visit_field_boolean(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_boolean");
        case DB::DeltaPrimitiveType::Byte:
            return unwrap(ffi::visit_field_byte(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_byte");
        case DB::DeltaPrimitiveType::Short:
            return unwrap(ffi::visit_field_short(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_short");
        case DB::DeltaPrimitiveType::Integer:
            return unwrap(ffi::visit_field_integer(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_integer");
        case DB::DeltaPrimitiveType::Long:
            return unwrap(ffi::visit_field_long(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_long");
        case DB::DeltaPrimitiveType::Float:
            return unwrap(ffi::visit_field_float(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_float");
        case DB::DeltaPrimitiveType::Double:
            return unwrap(ffi::visit_field_double(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_double");
        case DB::DeltaPrimitiveType::String:
            return unwrap(ffi::visit_field_string(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_string");
        case DB::DeltaPrimitiveType::Date:
            return unwrap(ffi::visit_field_date(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_date");
        case DB::DeltaPrimitiveType::Timestamp:
            return unwrap(ffi::visit_field_timestamp(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_timestamp");
        case DB::DeltaPrimitiveType::Decimal:
            return unwrap(
                ffi::visit_field_decimal(
                    state, name_slice,
                    static_cast<uint8_t>(DB::getDecimalPrecision(*type)),
                    static_cast<uint8_t>(DB::getDecimalScale(*type)),
                    nullable,
                    &KernelUtils::allocateError),
                "visit_field_decimal");
    }
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unhandled DeltaPrimitiveType for `{}`", type->getName());
}

/// Stored in `EngineSchema::visitor` and called once by the kernel; the anonymous namespace gives it internal linkage.
uintptr_t visitClickHouseSchema(void * schema_void, ffi::KernelSchemaVisitorState * state)
{
    auto * ctx = static_cast<KernelCreateSchemaState *>(schema_void);
    try
    {
        std::vector<uintptr_t> field_ids;
        field_ids.reserve(ctx->schema_list->size());
        for (const auto & col : *ctx->schema_list)
            field_ids.push_back(visitFieldFromClickHouseType(state, col.name, col.type));

        /// Top-level struct has an empty name; the kernel ignores it for the root schema. Keep the backing
        /// string alive so the `KernelStringSlice` does not dangle (it points into the string's buffer).
        static const std::string empty_name;
        return KernelUtils::unwrapResult(
            ffi::visit_field_struct(
                state, KernelUtils::toDeltaString(empty_name), field_ids.data(), field_ids.size(),
                /* nullable */ false,
                &KernelUtils::allocateError),
            "visit_field_struct(top-level)");
    }
    catch (...)
    {
        /// A C++ exception must not unwind through the kernel's Rust frames; capture it and return a sentinel for the caller to rethrow.
        ctx->exception = std::current_exception();
        return 0;
    }
}

}

/// Recursively check that a ClickHouse type maps to a round-tripping Delta type, throwing otherwise.
static void validateClickHouseTypeForDeltaCreate(const DB::DataTypePtr & full_type)
{
    DB::DataTypePtr type = full_type->isNullable() ? DB::removeNullable(full_type) : full_type;
    switch (type->getTypeId())
    {
        case DB::TypeIndex::Array:
            validateClickHouseTypeForDeltaCreate(assert_cast<const DB::DataTypeArray &>(*type).getNestedType());
            return;
        case DB::TypeIndex::Map:
        {
            const auto & map_type = assert_cast<const DB::DataTypeMap &>(*type);
            validateClickHouseTypeForDeltaCreate(map_type.getKeyType());
            validateClickHouseTypeForDeltaCreate(map_type.getValueType());
            return;
        }
        case DB::TypeIndex::Tuple:
        {
            const auto & elements = assert_cast<const DB::DataTypeTuple &>(*type).getElements();
            if (elements.empty())
                throw DB::Exception(
                    DB::ErrorCodes::NOT_IMPLEMENTED,
                    "DeltaLake does not support an empty Tuple/struct type for CREATE TABLE");
            for (const auto & element : elements)
                validateClickHouseTypeForDeltaCreate(element);
            return;
        }
        default:
            /// Throws `NOT_IMPLEMENTED` for any leaf type that cannot round-trip through Delta metadata.
            DB::classifyDeltaPrimitive(type);
            return;
    }
}

void validateSchemaForDeltaCreate(const DB::NamesAndTypesList & schema)
{
    for (const auto & column : schema)
        validateClickHouseTypeForDeltaCreate(column.type);
}

ffi::EngineSchema buildKernelEngineSchema(KernelCreateSchemaState & state)
{
    /// `schema` is an opaque `void *` the kernel never mutates; the visitor reads `state.schema_list` and stores any error in `state.exception`.
    return ffi::EngineSchema{
        /* schema */  &state,
        /* visitor */ &visitClickHouseSchema,
    };
}

}

#endif
