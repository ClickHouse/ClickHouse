#pragma once
#include <Common/Logger.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDecimalBase.h>

#include <delta_kernel_ffi.hpp>

namespace DeltaLake
{

namespace
{
    DB::TypeIndex getTypeIndex(const DB::DataTypePtr & type)
    {
        if (!type->isNullable())
            return type->getTypeId();

        const auto * nullable = assert_cast<const DB::DataTypeNullable *>(type.get());
        return nullable->getNestedType()->getTypeId();
    }

    DB::DataTypePtr getTypeOrNestedType(const DB::DataTypePtr & type)
    {
        if (!type->isNullable())
            return type;

        const auto * nullable = assert_cast<const DB::DataTypeNullable *>(type.get());
        return nullable->getNestedType();
    }
}

class EngineSchema : public ffi::EngineSchema
{
public:
    EngineSchema(const DB::NamesAndTypesList & requested_columns_)
        : requested_columns(requested_columns_)
    {
        schema = this;
        visitor = &visitSchema;
    }

    const DB::LoggerPtr & logger() { return log; }

private:
    DB::NamesAndTypesList requested_columns;
    DB::LoggerPtr log = getLogger("EngineSchema");
    std::exception_ptr visitor_exception;

    static uintptr_t visitSchema(void * data, ffi::KernelSchemaVisitorState * state)
    {
        auto * schema = static_cast<EngineSchema *>(data);
        try
        {
            std::vector<uintptr_t> field_ids;
            for (const auto & column : schema->requested_columns)
            {
                uintptr_t col_id = schema->visitSchemaItem(column, state);
                if (col_id == 0)
                {
                    /// Error will have been printed above
                    return 0;
                }
                field_ids.push_back(col_id);
            }
            LOG_TEST(schema->logger(), "Field ids: {}", field_ids.size());
            const uintptr_t * field_ids_ptr = field_ids.empty() ? nullptr : field_ids.data();
            return KernelUtils::unwrapResult(
                visit_field_struct(
                    state,
                    KernelUtils::toDeltaString("s"), /// Name does not matter
                    field_ids_ptr,
                    field_ids.size(),
                    /* nullable */false,
                    &KernelUtils::allocateError),
                "visit_field_struct");
        }
        catch (...)
        {
            schema->visitor_exception = std::current_exception();
            return 0;
        }
    }

    using SimpleFieldVisitor = ffi::ExternResult<uintptr_t> (*)(
        ffi::KernelSchemaVisitorState * state,
        ffi::KernelStringSlice name,
        bool nullable,
        ffi::AllocateErrorFn allocate_error);

    uintptr_t visitSchemaItem(
        const DB::NameAndTypePair & column, ffi::KernelSchemaVisitorState * state)
    {
        LOG_TEST(log, "Column name: {}, type: {}", column.name, column.type->getName());

        const auto name = KernelUtils::toDeltaString(column.name);
        const bool is_nullable = column.type->isNullable();
        const auto type_id = getTypeIndex(column.type);
        const auto is_bool = DB::isBool(column.type);

        auto apply = [&](SimpleFieldVisitor visitor) -> uintptr_t
        {
            return KernelUtils::unwrapResult(
                visitor(state, name, is_nullable, &KernelUtils::allocateError),
                "visit_field_*");
        };

        uintptr_t result;
        if (is_bool)
        {
            result = apply(&ffi::visit_field_boolean);
        }
        else if (type_id == DB::TypeIndex::String)
        {
            result = apply(&ffi::visit_field_string);
            //result = apply(&ffi::visit_field_binary);
        }
        else if (type_id == DB::TypeIndex::Int32 || type_id == DB::TypeIndex::UInt16)
        {
            result = apply(&ffi::visit_field_integer);
        }
        else if (type_id == DB::TypeIndex::Int16 || type_id == DB::TypeIndex::UInt8)
        {
            result = apply(&ffi::visit_field_short);
        }
        else if (type_id == DB::TypeIndex::Int8)
        {
            result = apply(&ffi::visit_field_byte);
        }
        else if (type_id == DB::TypeIndex::Int64 || type_id == DB::TypeIndex::UInt32)
        {
            result = apply(&ffi::visit_field_long);
        }
        else if (type_id == DB::TypeIndex::Float32)
        {
            result = apply(&ffi::visit_field_float);
        }
        else if (type_id == DB::TypeIndex::Float64)
        {
            result = apply(&ffi::visit_field_double);
        }
        else if (type_id == DB::TypeIndex::Date)
        {
            result = apply(&ffi::visit_field_date);
        }
        else if (type_id == DB::TypeIndex::DateTime)
        {
            result = apply(&ffi::visit_field_timestamp);
        }
        else if (type_id == DB::TypeIndex::DateTime64)
        {
            result = apply(&ffi::visit_field_timestamp_ntz);
        }
        else if (type_id == DB::TypeIndex::Decimal32)
        {
            auto * decimal = assert_cast<const DB::DataTypeDecimalBase<DB::Decimal32> *>(
                getTypeOrNestedType(column.type).get());

            result = KernelUtils::unwrapResult(
                ffi::visit_field_decimal(
                    state,
                    name,
                    decimal->getPrecision(),
                    decimal->getScale(),
                    is_nullable,
                    &KernelUtils::allocateError),
                "visit_field_decimal");
        }
        //} else if (strcmp(item->type, "array") == 0)
        //{
        //    SchemaItemList child_list = cschema->builder->lists[item->children];
        //    // an array should always have 1 child
        //    if (child_list.len != 1) {
        //    printf("[ERROR] Invalid array child list");
        //    return 0;
        //    }
        //    uintptr_t child_visit_id = visit_schema_item(&child_list.list[0], state, cschema);
        //    if (child_visit_id == 0) {
        //    // previous visit will have printed the issue
        //    return 0;
        //    }
        //    result = ffi::visit_field_array(state, name, child_visit_id, is_nullable, &KernelUtils::allocateError);
        //} else if (strcmp(item->type, "map") == 0)
        //{
        //    SchemaItemList child_list = cschema->builder->lists[item->children];
        //    // an map should always have 2 children
        //    if (child_list.len != 2) {
        //    printf("[ERROR] Invalid map child list");
        //    return 0;
        //    }
        //    uintptr_t key_visit_id = visit_schema_item(&child_list.list[0], state, cschema);
        //    if (key_visit_id == 0) {
        //    // previous visit will have printed the issue
        //    return 0;
        //    }
        //    uintptr_t val_visit_id = visit_schema_item(&child_list.list[1], state, cschema);
        //    if (val_visit_id == 0) {
        //    // previous visit will have printed the issue
        //    return 0;
        //    }
        //    result = ffi::visit_field_map(state, name, key_visit_id, val_visit_id, is_nullable, &KernelUtils::allocateError);
        //}
        //else if (strcmp(item->type, "struct") == 0)
        //{
        //    SchemaItemList child_list = cschema->builder->lists[item->children];
        //    uintptr_t child_visit_ids[child_list.len];
        //    for (uint32_t i = 0; i < child_list.len; i++) {
        //    // visit all the children
        //    SchemaItem *item = &child_list.list[i];
        //    uintptr_t child_id = visit_schema_item(item, state, cschema);
        //    if (child_id == 0) {
        //        // previous visit will have printed the issue
        //        return 0;
        //    } 
        //    child_visit_ids[i] = child_id;
        //    }
        //    result = ffi::visit_field_struct(
        //    state,
        //    name,
        //    child_visit_ids,
        //    child_list.len,
        //    is_nullable,
        //    &KernelUtils::allocateError);
        //}
        else
        {
            LOG_ERROR(log, "Unsupported type: {}", column.type->getName());
            return 0;
        }

        return result;
    }
};

}
