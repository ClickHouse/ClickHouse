#include "getSchemaFromSnapshot.h"

#include <Core/TypeId.h>
#include <Core/getDataTypeFromTypeIndex.h>
#include <Common/logger_useful.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>

#include "delta_kernel_ffi.hpp"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DeltaLake
{

class SchemaVisitorData
{
    friend class SchemaVisitor;
public:
    DB::NamesAndTypesList getSchemaResult();

private:
    DB::DataTypes getDataTypesFromTypeList(size_t list_idx, bool nullable);

    struct Field
    {
        Field(const std::string & name_, const DB::TypeIndex & type_)
            : name(name_), type(type_) {}

        /// Column name.
        const std::string name;
        /// Column type.
        const DB::TypeIndex type;
        /// If type is complex, whether it can contain nullable values.
        bool value_contains_null;
        /// If type is complex, list id of the child list
        size_t child_list_id;
        /// If type is simple, a flag to distringuish
        /// between Bool and UInt8 in case of TypeIndex::UInt8.
        bool is_bool = false;
        /// For Decimal.
        size_t precision = 0;
        size_t scale = 0;
    };
    using Fields = std::vector<Field>;

    /// See delta-kernel-rs/fii/src/schema.rs for type_lists explanation.
    std::unordered_map<size_t, std::unique_ptr<Fields>> type_lists;
    /// Global counter for type lists.
    size_t list_counter = 0;

    const LoggerPtr log = getLogger("SchemaVisitor");
};

class SchemaVisitor
{
public:
    static DB::NamesAndTypesList visit(ffi::SharedSnapshot * snapshot, SchemaVisitorData & data)
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

        size_t result = ffi::visit_schema(snapshot, &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        return data.getSchemaResult();
    }

private:
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

    template <DB::TypeIndex type, bool is_bool = false>
    static void simpleTypeVisitor(void * data, uintptr_t sibling_list_id, ffi::KernelStringSlice name)
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

        LOG_TEST(
            state->log,
            "List id: {}, column name: {}, type: {}",
            sibling_list_id, column_name, type);

        SchemaVisitorData::Field field(column_name, std::move(type));
        field.is_bool = is_bool;

        it->second->push_back(std::move(field));
    }

    static void decimalTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
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

        LOG_TEST(
            state->log,
            "List id: {}, column name: {}, type: {}",
            sibling_list_id, column_name, type);

        SchemaVisitorData::Field field(column_name, type);
        field.precision = precision;
        field.scale = scale;

        it->second->push_back(std::move(field));
    }

    static void arrayTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool contains_null,
        uintptr_t child_list_id)
    {
        return listBasedTypeVisitor<DB::TypeIndex::Array>(data, sibling_list_id, name, contains_null, child_list_id);
    }

    static void tupleTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        uintptr_t child_list_id)
    {
        return listBasedTypeVisitor<DB::TypeIndex::Tuple>(data, sibling_list_id, name, false, child_list_id);
    }

    static void mapTypeVisitor(
        void *data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool value_contains_null,
        uintptr_t child_list_id)
    {
        return listBasedTypeVisitor<DB::TypeIndex::Map>(data, sibling_list_id, name, value_contains_null, child_list_id);
    }

    template <DB::TypeIndex type>
    static void listBasedTypeVisitor(
        void * data,
        uintptr_t sibling_list_id,
        ffi::KernelStringSlice name,
        bool value_contains_null,
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

        LOG_TEST(
            state->log,
            "List id: {}, column name: {}, type: {}, "
            "value contains null: {}, child list id: {}",
            sibling_list_id, column_name, type, value_contains_null, child_list_id);

        SchemaVisitorData::Field field(column_name, std::move(type));
        field.child_list_id = child_list_id;
        field.value_contains_null = value_contains_null;

        it->second->push_back(field);
    }

};

DB::NamesAndTypesList SchemaVisitorData::getSchemaResult()
{
    const auto types = getDataTypesFromTypeList(0, true);
    chassert(types.size() == type_lists[0]->size());

    std::list<DB::NameAndTypePair> result;
    for (size_t i = 0; i < types.size(); ++i)
    {
        result.emplace_back((*type_lists[0])[i].name, types[i]);
    }
    return DB::NamesAndTypesList(result.begin(), result.end());
}

DB::DataTypes SchemaVisitorData::getDataTypesFromTypeList(size_t list_idx, bool nullable)
{
    DB::DataTypes types;
    for (const auto & field : *type_lists[list_idx])
    {
        if (field.is_bool)
        {
            auto type = DB::DataTypeFactory::instance().get("Bool");
            if (nullable)
                type = std::make_shared<DB::DataTypeNullable>(type);

            types.push_back(type);
        }
        else if (field.type == DB::TypeIndex::Decimal32)
        {
            auto type = DB::createDecimal<DB::DataTypeDecimal>(field.precision, field.scale);
            if (nullable)
                type = std::make_shared<DB::DataTypeNullable>(type);

            types.push_back(type);
        }
        else if (DB::isSimpleDataType(field.type))
        {
            auto type = DB::getSimpleDataTypeFromTypeIndex(field.type);
            if (nullable)
                type = std::make_shared<DB::DataTypeNullable>(type);

            types.push_back(type);
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
            if (which.isTuple())
            {
                auto child_types = getDataTypesFromTypeList(field.child_list_id, field.value_contains_null);
                types.push_back(std::make_shared<DB::DataTypeTuple>(child_types));
            }
            else if (which.isArray())
            {
                auto child_types = getDataTypesFromTypeList(field.child_list_id, field.value_contains_null);
                if (child_types.size() != 1)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Unexpected number of types in array: {}",
                        child_types.size());
                }

                types.push_back(std::make_shared<DB::DataTypeArray>(child_types[0]));
            }
            else if (which.isMap())
            {
                auto child_types = getDataTypesFromTypeList(field.child_list_id, field.value_contains_null);
                if (child_types.size() != 2)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Unexpected number of types in array: {}",
                        child_types.size());
                }
                types.push_back(std::make_shared<DB::DataTypeMap>(child_types[0], child_types[1]));
            }
            else
            {
                throw DB::Exception(
                    DB::ErrorCodes::NOT_IMPLEMENTED,
                    "Unsupported complex data type: {}", field.child_list_id);
            }
        }
    }
    return types;
}

DB::NamesAndTypesList getSchemaFromSnapshot(ffi::SharedSnapshot * snapshot)
{
    SchemaVisitorData data;
    SchemaVisitor::visit(snapshot, data);
    return data.getSchemaResult();
}

}
