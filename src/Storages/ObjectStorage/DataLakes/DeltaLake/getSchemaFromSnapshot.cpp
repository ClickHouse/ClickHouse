#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <DataTypes/DataTypeFactory.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>

#include <base/scope_guard.h>
#include <Core/TypeId.h>
#include <Common/logger_useful.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>

#include <IO/WriteHelpers.h>

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
    /// `engine_` is required by v0.23.0 FFI helpers such as `ffi::get_from_string_map`.
    /// Pass `nullptr` for visit paths that don't need engine-bound FFI calls
    /// (e.g. partition column extraction).
    explicit SchemaVisitorData(ffi::SharedExternEngine * engine_ = nullptr) : engine(engine_) {}

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
        bool value_contains_null;
        /// If type is complex (array, map, struct), list id of the child list.
        size_t child_list_id;

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
    }

    static void visitReadSchema(ffi::SharedScan * scan, SchemaVisitorData & data)
    {
        KernelSharedSchema schema(ffi::scan_physical_schema(scan));
        auto visitor = createVisitor(data);
        [[maybe_unused]] size_t result = ffi::visit_schema(schema.get(), &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));
    }

    static void visitWriteSchema(ffi::SharedWriteContext * write_context, SchemaVisitorData & data)
    {
        KernelSharedSchema schema(ffi::get_write_schema(write_context));
        auto visitor = createVisitor(data);
        [[maybe_unused]] size_t result = ffi::visit_schema(schema.get(), &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));
    }

    static void visitPartitionColumns(ffi::SharedSnapshot * snapshot, SchemaVisitorData & data)
    {
        KernelStringSliceIterator partition_columns_iter(ffi::get_partition_columns(snapshot));
        while (ffi::string_slice_next(partition_columns_iter.get(), &data, &visitPartitionColumn)) {}
    }

    static void visitSchema(ffi::SharedSchema * schema, SchemaVisitorData & data)
    {
        auto visitor = createVisitor(data);
        [[maybe_unused]] size_t result = ffi::visit_schema(schema, &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));
    }

private:
    static ffi::EngineSchemaVisitor createVisitor(SchemaVisitorData & data)
    {
        ffi::EngineSchemaVisitor visitor;
        visitor.data = &data;
        visitor.make_field_list = &makeFieldList;

        visitor.visit_boolean = &simpleTypeVisitor<DB::TypeIndex::Int8, true>;
        visitor.visit_string = &simpleTypeVisitor<DB::TypeIndex::String>;
        visitor.visit_long = &simpleTypeVisitor<DB::TypeIndex::Int64>;
        visitor.visit_integer = &simpleTypeVisitor<DB::TypeIndex::Int32>;
        visitor.visit_short = &simpleTypeVisitor<DB::TypeIndex::Int16>;
        visitor.visit_byte = &simpleTypeVisitor<DB::TypeIndex::Int8>;
        visitor.visit_float = &simpleTypeVisitor<DB::TypeIndex::Float32>;
        visitor.visit_double = &simpleTypeVisitor<DB::TypeIndex::Float64>;
        visitor.visit_binary = &simpleTypeVisitor<DB::TypeIndex::String>;
        visitor.visit_date = &simpleTypeVisitor<DB::TypeIndex::Date32>;
        visitor.visit_timestamp = &simpleTypeVisitor<DB::TypeIndex::DateTime64>;
        visitor.visit_timestamp_ntz = &simpleTypeVisitor<DB::TypeIndex::DateTime64>;

        visitor.visit_array = &arrayTypeVisitor;
        visitor.visit_struct = &tupleTypeVisitor;
        visitor.visit_map = &mapTypeVisitor;
        visitor.visit_decimal = &decimalTypeVisitor;

        return visitor;
    }

    static void visitPartitionColumn(void * data, ffi::KernelStringSlice slice)
    {
        SchemaVisitorData * state = static_cast<SchemaVisitorData *>(data);
        state->partition_columns.push_back(KernelUtils::fromDeltaString(slice));
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
    SchemaVisitorData data;
    SchemaVisitor::visitPartitionColumns(snapshot, data);
    return data.getPartitionColumns();
}

DB::NamesAndTypesList convertToClickHouseSchema(ffi::SharedSchema * schema, ffi::SharedExternEngine * engine)
{
    SchemaVisitorData data(engine);
    SchemaVisitor::visitSchema(schema, data);
    return data.getSchemaResult().names_and_types;
}

/// =============================================================================
/// CH -> kernel: schema visitor used by `ffi::get_create_table_builder`.
///
/// The kernel calls the function-pointer stored in `EngineSchema::visitor`
/// once, passing back the opaque `schema` field as the user data. The visitor
/// must call `ffi::visit_field_*` for every leaf type to register field IDs,
/// then call `ffi::visit_field_struct` for the top-level struct (anonymous
/// name) and return its ID.
/// =============================================================================

namespace
{

uintptr_t visitFieldFromClickHouseType(
    ffi::KernelSchemaVisitorState * state,
    const std::string & name,
    const DB::DataTypePtr & full_type);

uintptr_t visitElementFromClickHouseType(
    ffi::KernelSchemaVisitorState * state,
    const DB::DataTypePtr & full_type);

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

    switch (type->getTypeId())
    {
        case DB::TypeIndex::Int8:
            return unwrap(ffi::visit_field_byte(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_byte");
        case DB::TypeIndex::Int16:
            return unwrap(ffi::visit_field_short(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_short");
        case DB::TypeIndex::Int32:
            return unwrap(ffi::visit_field_integer(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_integer");
        case DB::TypeIndex::Int64:
            return unwrap(ffi::visit_field_long(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_long");
        case DB::TypeIndex::Float32:
            return unwrap(ffi::visit_field_float(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_float");
        case DB::TypeIndex::Float64:
            return unwrap(ffi::visit_field_double(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_double");
        case DB::TypeIndex::UInt8:
            /// ClickHouse stores Bool as UInt8 today.
            return unwrap(ffi::visit_field_boolean(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_boolean");
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
            return unwrap(ffi::visit_field_string(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_string");
        case DB::TypeIndex::Date:
        case DB::TypeIndex::Date32:
            return unwrap(ffi::visit_field_date(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_date");
        case DB::TypeIndex::DateTime:
        case DB::TypeIndex::DateTime64:
            return unwrap(ffi::visit_field_timestamp(state, name_slice, nullable, &KernelUtils::allocateError), "visit_field_timestamp");
        case DB::TypeIndex::Array:
        {
            const auto & array_type = assert_cast<const DB::DataTypeArray &>(*type);
            auto element_id = visitElementFromClickHouseType(state, array_type.getNestedType());
            return unwrap(
                ffi::visit_field_array(state, name_slice, element_id, nullable, &KernelUtils::allocateError),
                "visit_field_array");
        }
        case DB::TypeIndex::Map:
        {
            const auto & map_type = assert_cast<const DB::DataTypeMap &>(*type);
            auto key_id = visitElementFromClickHouseType(state, map_type.getKeyType());
            auto value_id = visitElementFromClickHouseType(state, map_type.getValueType());
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
        case DB::TypeIndex::Decimal32:
        case DB::TypeIndex::Decimal64:
        case DB::TypeIndex::Decimal128:
        case DB::TypeIndex::Decimal256:
            return unwrap(
                ffi::visit_field_decimal(
                    state, name_slice,
                    static_cast<uint8_t>(DB::getDecimalPrecision(*type)),
                    static_cast<uint8_t>(DB::getDecimalScale(*type)),
                    nullable,
                    &KernelUtils::allocateError),
                "visit_field_decimal");
        default:
            throw DB::Exception(
                DB::ErrorCodes::NOT_IMPLEMENTED,
                "ClickHouse type `{}` cannot be mapped to a Delta Lake type for CREATE TABLE",
                type->getName());
    }
}

uintptr_t visitElementFromClickHouseType(ffi::KernelSchemaVisitorState * state, const DB::DataTypePtr & full_type)
{
    /// For array elements / map keys & values the kernel expects a synthetic anonymous
    /// field. We reuse `visitFieldFromClickHouseType` with an empty name.
    return visitFieldFromClickHouseType(state, /* name */ "", full_type);
}

extern "C" uintptr_t kernelEngineSchemaVisitorTrampoline(void * schema_void, ffi::KernelSchemaVisitorState * state)
{
    const auto * schema_list = static_cast<const DB::NamesAndTypesList *>(schema_void);
    std::vector<uintptr_t> field_ids;
    field_ids.reserve(schema_list->size());
    for (const auto & col : *schema_list)
        field_ids.push_back(visitFieldFromClickHouseType(state, col.name, col.type));

    /// Top-level struct has an empty name; the kernel ignores it for the root schema.
    auto empty_name = KernelUtils::toDeltaString("");
    return KernelUtils::unwrapResult(
        ffi::visit_field_struct(
            state, empty_name, field_ids.data(), field_ids.size(),
            /* nullable */ false,
            &KernelUtils::allocateError),
        "visit_field_struct(top-level)");
}

}

ffi::EngineSchema buildKernelEngineSchema(const DB::NamesAndTypesList & schema_list)
{
    /// `schema` is `void *`; the kernel never mutates it, but the FFI struct field
    /// is non-const, so cast accordingly. The visitor reads only — there is no
    /// thread safety concern.
    return ffi::EngineSchema{
        /* schema */  const_cast<DB::NamesAndTypesList *>(&schema_list),
        /* visitor */ &kernelEngineSchemaVisitorTrampoline,
    };
}

}

#endif
