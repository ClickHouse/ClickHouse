#include <Analyzer/SetUtils.h>

#include <Core/Block.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>

#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_ELEMENT_OF_SET;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int UNKNOWN_ELEMENT_OF_ENUM;
extern const int LOGICAL_ERROR;

}

namespace
{

size_t getCompoundTypeDepth(const IDataType & type)
{
    size_t depth = 0;

    const IDataType * current_type = &type;

    while (true)
    {
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(current_type))
        {
            current_type = nullable_type->getNestedType().get();
            continue;
        }

        WhichDataType which_type(*current_type);

        if (which_type.isArray())
        {
            current_type = assert_cast<const DataTypeArray &>(*current_type).getNestedType().get();
            ++depth;
        }
        else if (which_type.isTuple())
        {
            const auto & tuple_elements = assert_cast<const DataTypeTuple &>(*current_type).getElements();
            ++depth;
            if (tuple_elements.empty())
                break;
            current_type = tuple_elements.front().get();
        }
        else
        {
            break;
        }
    }

    return depth;
}

/// The `convertFieldToTypeStrict` is used to prevent unexpected results in case of conversion with loss of precision.
/// Example: `SELECT 33.3 :: Decimal(9, 1) AS a WHERE a IN (33.33 :: Decimal(9, 2))`
/// 33.33 in the set is converted to 33.3, but it is not equal to 33.3 in the column, so the result should still be empty.
/// We can not include values that don't represent any possible value from the type of filtered column to the set.
std::optional<Field> convertFieldToTypeCheckEnum(
    const Field & from_value, const IDataType & from_type, const IDataType & to_type, bool forbid_unknown_enum_values)
{
    try
    {
        return convertFieldToTypeStrict(from_value, from_type, to_type);
    }
    catch (const Exception & e)
    {
        if (!forbid_unknown_enum_values && isEnum(to_type) && e.code() == ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM)
            return {};
        throw;
    }
}

/// rhs_collection can be:
/// Array: [1, 2, 3] or [(1, 2), (3, 4)] or [NULL]
/// Tuple: (1, 2, 3) or ((1, 2), (3, 4), (5, 6), (7, 8)) or (NULL, NULL)
template <typename Collection>
ColumnsWithTypeAndName createBlockFromCollection(
    const Collection & rhs_collection,
    const DataTypes & rhs_types,
    const DataTypes & lhs_unpacked_types,
    bool lhs_is_nullable,
    GetSetElementParams params)
{
    chassert(rhs_collection.size() == rhs_types.size());

    size_t num_elements = lhs_unpacked_types.size();

    /// Fast path
    if (num_elements == 1)
    {
        MutableColumnPtr column = lhs_unpacked_types[0]->createColumn();
        column->reserve(rhs_collection.size());

        for (size_t collection_index = 0; collection_index < rhs_collection.size(); ++collection_index)
        {
            const auto & rhs_element = rhs_collection[collection_index];

            auto field = convertFieldToTypeCheckEnum(
                rhs_element, *rhs_types[collection_index], *lhs_unpacked_types[0], params.forbid_unknown_enum_values);

            bool need_insert_null = params.transform_null_in && column->isNullable();
            if (field && (!field->isNull() || need_insert_null))
                column->insert(*field);
        }

        ColumnsWithTypeAndName res(1);
        res[0].type = lhs_unpacked_types[0];
        res[0].column = std::move(column);
        return res;
    }

    /// If lhs is Nullable(Tuple), then since we cast RHS elements to lhs types, we must wrap the final Tuple column into Nullable.
    const bool construct_nullable_tuple_column_later = lhs_is_nullable;

    /// Skip: if conversion failed between lhs_tuple and rhs_element and does not raise
    ///       exception (like unknown enum value and `validate_enum_literals_in_operators = 0`)
    /// InsertNull: if rhs_element is NULL
    /// InsertValue: if conversion succeeded

    enum class RowAction : UInt8
    {
        Skip,
        InsertNull,
        InsertValue,
    };

    std::vector<RowAction> row_actions;
    if (construct_nullable_tuple_column_later)
        row_actions.assign(rhs_collection.size(), RowAction::Skip);

    MutableColumns columns(num_elements);
    for (size_t i = 0; i < num_elements; ++i)
    {
        columns[i] = lhs_unpacked_types[i]->createColumn();
        columns[i]->reserve(rhs_collection.size());
    }

    Row converted_rhs_element_unpacked;

    for (size_t collection_index = 0; collection_index < rhs_collection.size(); ++collection_index)
    {
        const auto & rhs_element = rhs_collection[collection_index];

        if (rhs_element.isNull())
        {
            if (construct_nullable_tuple_column_later)
                row_actions[collection_index] = RowAction::InsertNull;

            continue;
        }

        if (rhs_element.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type in set. Expected tuple, got {}", rhs_element.getTypeName());

        const auto & rhs_element_as_tuple = rhs_element.template safeGet<Tuple>();

        const DataTypePtr & rhs_element_type = rhs_types[collection_index];

        const DataTypeTuple * tuple_type = nullptr;
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(rhs_element_type.get()))
        {
            /// RHS type is Nullable(Tuple(...))
            tuple_type = typeid_cast<const DataTypeTuple *>(nullable_type->getNestedType().get());
        }
        else
        {
            /// RHS type is Tuple(...)
            tuple_type = typeid_cast<const DataTypeTuple *>(rhs_element_type.get());
        }

        if (!tuple_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Tuple or Nullable(Tuple) type");

        const DataTypes & rhs_element_unpacked_types = tuple_type->getElements();

        size_t tuple_size = rhs_element_as_tuple.size();

        if (tuple_size != num_elements)
            throw Exception(
                ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Incorrect size of tuple in set: {} instead of {}", tuple_size, num_elements);

        if (converted_rhs_element_unpacked.empty())
            converted_rhs_element_unpacked.resize(tuple_size);

        size_t i = 0;
        for (; i < tuple_size; ++i)
        {
            auto converted_field = convertFieldToTypeCheckEnum(
                rhs_element_as_tuple[i], *rhs_element_unpacked_types[i], *lhs_unpacked_types[i], params.forbid_unknown_enum_values);
            if (!converted_field)
                break;
            converted_rhs_element_unpacked[i] = std::move(*converted_field);

            bool need_insert_null = params.transform_null_in && lhs_unpacked_types[i]->isNullable();
            if (converted_rhs_element_unpacked[i].isNull() && !need_insert_null)
                break;
        }

        if (i == tuple_size)
        {
            for (i = 0; i < tuple_size; ++i)
                columns[i]->insert(converted_rhs_element_unpacked[i]);

            if (construct_nullable_tuple_column_later)
                row_actions[collection_index] = RowAction::InsertValue;
        }
    }

    if (!construct_nullable_tuple_column_later)
    {
        ColumnsWithTypeAndName res(num_elements);
        for (size_t i = 0; i < num_elements; ++i)
        {
            res[i].type = lhs_unpacked_types[i];
            res[i].column = std::move(columns[i]);
        }

        return res;
    }

#ifndef NDEBUG
    if (construct_nullable_tuple_column_later)
    {
        chassert(!row_actions.empty());
        chassert(row_actions.size() == rhs_collection.size());
    }

    if (construct_nullable_tuple_column_later && !columns.empty())
    {
        const auto rows_in_columns = columns.front()->size();
        for (size_t i = 1; i < num_elements; ++i)
            chassert(columns[i]->size() == rows_in_columns);
    }
#endif

    /// If we are here, then it means that lhs is Nullable(Tuple(..)) and we have NULLs in the rhs collection
    DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(lhs_unpacked_types);
    DataTypePtr nullable_tuple_type = std::make_shared<DataTypeNullable>(tuple_type);
    MutableColumnPtr nullable_tuple_column = nullable_tuple_type->createColumn();
    nullable_tuple_column->reserve(rhs_collection.size());

    auto & column_nullable = assert_cast<ColumnNullable &>(*nullable_tuple_column);
    auto & nested_tuple_column = assert_cast<ColumnTuple &>(column_nullable.getNestedColumn());
    auto & null_map = column_nullable.getNullMapColumn();

    /// Track current position in the pre-built columns so that elements are in the same order in the final column as in rhs_collection
    size_t non_null_pos = 0;

    for (size_t collection_index = 0; collection_index < rhs_collection.size(); ++collection_index)
    {
        switch (row_actions[collection_index])
        {
            case RowAction::InsertNull: {
                nested_tuple_column.insertDefault();
                null_map.getData().push_back(1);
                break;
            }

            case RowAction::InsertValue: {
                chassert(non_null_pos < columns.front()->size());
                for (size_t i = 0; i < num_elements; ++i)
                    nested_tuple_column.getColumn(i).insertFrom(*columns[i], non_null_pos);

                null_map.insertDefault();
                ++non_null_pos;
                break;
            }

            case RowAction::Skip: {
                /// No row for this rhs element (conversion failure, or NULL that shouldn't be transformed).
                break;
            }
        }
    }

    chassert(non_null_pos == columns.front()->size());

    ColumnsWithTypeAndName res(1);
    res[0].type = nullable_tuple_type;
    res[0].column = std::move(nullable_tuple_column);
    return res;
}

bool hasNullInCollection(const Field & rhs, const WhichDataType & rhs_which_type)
{
    if (rhs_which_type.isArray())
    {
        const auto & rhs_array = rhs.safeGet<Array>();
        for (const auto & elem : rhs_array)
            if (elem.isNull())
                return true;
    }
    else if (rhs_which_type.isTuple())
    {
        const auto & rhs_tuple = rhs.safeGet<Tuple>();
        for (const auto & elem : rhs_tuple)
            if (elem.isNull())
                return true;
    }
    return false;
}

bool hasTupleInCollection(const Field & rhs, const WhichDataType & rhs_which_type)
{
    if (rhs_which_type.isArray())
    {
        const auto & rhs_array = rhs.safeGet<Array>();
        for (const auto & elem : rhs_array)
            if (elem.getType() == Field::Types::Tuple)
                return true;
    }
    else if (rhs_which_type.isTuple())
    {
        const auto & rhs_tuple = rhs.safeGet<Tuple>();
        for (const auto & elem : rhs_tuple)
            if (elem.getType() == Field::Types::Tuple)
                return true;
    }
    return false;
}


}

/// Format: lhs IN rhs
/// Explanation of the setting: `transform_null_in`. First of all, it is only applicable if the lhs is nullable.
/// Then if lhs is nullable and `transform_null_in` is true, then NULLs from rhs are inserted into the result set as well.
/// Whereas, if `transform_null_in` is false, we pretend NULLs are not present in rhs at all (at level 1 or at level 2 for Tuple).
/// If `transform_null_in` is false, then `SELECT NULL IN (NULL, 1)` returns NULL, otherwise it returns true.

ColumnsWithTypeAndName getSetElementsForConstantValue(
    const DataTypePtr & lhs_expression_type, const Field & rhs, const DataTypePtr & rhs_type, GetSetElementParams params)
{
    DataTypes lhs_unpacked_types = {lhs_expression_type};

    bool lhs_is_tuple = false;
    /// Unpack if Tuple or Nullable(Tuple) and Tuple has more than 1 element
    const auto * lhs_nullable_type = typeid_cast<const DataTypeNullable *>(lhs_expression_type.get());
    if (const auto * lhs_tuple_type = typeid_cast<const DataTypeTuple *>(lhs_nullable_type ? lhs_nullable_type->getNestedType().get() : lhs_expression_type.get()))
    {
        lhs_is_tuple = true;
        /// Do not unpack if empty tuple or single element tuple
        if (lhs_tuple_type->getElements().size() > 1)
            lhs_unpacked_types = lhs_tuple_type->getElements();
    }
    bool lhs_is_nullable = (lhs_nullable_type != nullptr);

    for (auto & lhs_element_type : lhs_unpacked_types)
    {
        if (const auto * set_element_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(lhs_element_type.get()))
            lhs_element_type = set_element_low_cardinality_type->getDictionaryType();
    }


    auto buildFromArray = [&](const Field & rhs_value, const DataTypePtr & type)
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        const auto & rhs_array = rhs_value.safeGet<Array>();
        DataTypes rhs_types(rhs_array.size(), array_type->getNestedType());
        return createBlockFromCollection(rhs_array, rhs_types, lhs_unpacked_types, lhs_is_nullable, params);
    };

    auto buildFromTuple = [&](const Field & rhs_value, const DataTypePtr & type)
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        const auto & rhs_tuple = rhs_value.safeGet<Tuple>();
        const auto & rhs_types = tuple_type->getElements();
        return createBlockFromCollection(rhs_tuple, rhs_types, lhs_unpacked_types, lhs_is_nullable, params);
    };


    size_t lhs_type_depth = getCompoundTypeDepth(*lhs_expression_type);
    size_t rhs_type_depth = getCompoundTypeDepth(*rhs_type);

    /// CAST(NULL, `Nullable(Tuple(...))`) IN NULL
    if (lhs_type_depth == rhs_type_depth + 1)
    {
        if (rhs.isNull())
            return createBlockFromCollection(Array{rhs}, DataTypes{rhs_type}, lhs_unpacked_types, lhs_is_nullable, params);
    }
    else if (lhs_type_depth == rhs_type_depth)
    {
        WhichDataType rhs_which_type(rhs_type);

        bool is_null_in_rhs = hasNullInCollection(rhs, rhs_which_type);
        bool has_tuple_in_rhs = hasTupleInCollection(rhs, rhs_which_type);

        if (lhs_is_tuple && rhs_which_type.isArray() && is_null_in_rhs)
        {
            /// CAST(NULL, `Nullable(Tuple(...))`) IN [NULL, NULL, (...)]
            /// Tuple(...) IN [NULL, NULL, (...)]
            return buildFromArray(rhs, rhs_type);
        }

        if (lhs_is_tuple && rhs_which_type.isTuple() && is_null_in_rhs && (lhs_is_nullable || has_tuple_in_rhs))
        {
            /// CAST(NULL, `Nullable(Tuple(...))`) IN (NULL, NULL, ...)
            /// Tuple(...) IN (NULL, NULL, Tuple(...), ...)
            return buildFromTuple(rhs, rhs_type);
        }

        /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
        return createBlockFromCollection(Array{rhs}, DataTypes{rhs_type}, lhs_unpacked_types, lhs_is_nullable, params);
    }
    else if (lhs_type_depth + 1 == rhs_type_depth)
    {
        /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4))
        WhichDataType rhs_which_type(rhs_type);

        if (rhs_which_type.isArray())
            return buildFromArray(rhs, rhs_type);

        if (rhs_which_type.isTuple())
            return buildFromTuple(rhs, rhs_type);

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported type at the right-side of IN. Expected Array or Tuple or Nullable(Tuple). Actual {}",
            rhs_type->getName());
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Unsupported types for IN. First argument type {}. Second argument type {}",
        lhs_expression_type->getName(),
        rhs_type->getName());
}

}
