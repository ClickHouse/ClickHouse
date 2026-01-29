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
}

namespace
{

/// Unwrap Nullable to get to the underlying Tuple type.
/// Also unwrap LowCardinality for robustness, even though LowCardinality(Tuple) is not supported.
const DataTypeTuple * getTupleType(const DataTypePtr & type)
{
    const IDataType * current = type.get();

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(current))
        current = lc_type->getDictionaryType().get();

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(current))
        current = nullable_type->getNestedType().get();

    return typeid_cast<const DataTypeTuple *>(current);
}

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
    const Collection & rhs_collection, const DataTypes & rhs_types, const DataTypes & lhs_unpacked_types, GetSetElementParams params)
{
    chassert(rhs_collection.size() == rhs_types.size());

    size_t num_elements = lhs_unpacked_types.size();

    /// Fast path: single key column (lhs_unpacked_types.size() == 1)
    /// Special-case `Nullable(Tuple(...))`; otherwise generic scalar conversion
    if (num_elements == 1)
    {
        const auto & lhs_type = lhs_unpacked_types[0];
        MutableColumnPtr column = lhs_type->createColumn();
        column->reserve(rhs_collection.size());

        /// For `Nullable(Tuple(...))` we process tuple values element-by-element:
        /// - to correctly skip unknown enum literals when `validate_enum_literals_in_operators = 0`
        /// - to implement `transform_null_in = 0` semantics for NULLs inside tuple elements (skip such tuple values)
        ///
        /// Example:
        /// - CAST((NULL, 42) AS Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))) IN ((NULL, 42)) SETTINGS transform_null_in = 0
        ///   set elements: {} (tuple literal is skipped, because it has NULL element)
        const auto * lhs_nullable = typeid_cast<const DataTypeNullable *>(lhs_type.get());
        const auto * lhs_tuple = lhs_nullable ? typeid_cast<const DataTypeTuple *>(lhs_nullable->getNestedType().get()) : nullptr;

        /// Nullable(Tuple(...)) case
        if (lhs_tuple)
        {
            chassert(lhs_nullable);
            const auto & lhs_tuple_element_types = lhs_tuple->getElements();

            for (size_t collection_index = 0; collection_index < rhs_collection.size(); ++collection_index)
            {
                const auto & rhs_element = rhs_collection[collection_index];

                /// The NULL can be of any type but that's okay
                if (rhs_element.isNull())
                {
                    if (params.transform_null_in)
                        column->insert(Null{});
                    continue;
                }

                if (rhs_element.getType() != Field::Types::Tuple)
                    throw Exception(
                        ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type in set. Expected tuple, got {}", rhs_element.getTypeName());

                const auto & rhs_element_as_tuple = rhs_element.template safeGet<Tuple>();
                const size_t tuple_size = rhs_element_as_tuple.size();

                if (tuple_size != lhs_tuple_element_types.size())
                    throw Exception(
                        ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                        "Incorrect size of tuple in set: {} instead of {}",
                        tuple_size,
                        lhs_tuple_element_types.size());

                const DataTypePtr & rhs_element_type = rhs_types[collection_index];
                const DataTypeTuple * rhs_tuple_type = getTupleType(rhs_element_type);

                if (!rhs_tuple_type)
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                        "Invalid element type in set. Expected Tuple, got {}", rhs_element_type->getName());

                const DataTypes & rhs_tuple_element_types = rhs_tuple_type->getElements();

                Tuple converted_tuple;
                converted_tuple.reserve(tuple_size);

                bool skip_tuple_value = false;
                for (size_t i = 0; i < tuple_size; ++i)
                {
                    auto converted_field = convertFieldToTypeCheckEnum(
                        rhs_element_as_tuple[i],
                        *rhs_tuple_element_types[i],
                        *lhs_tuple_element_types[i],
                        params.forbid_unknown_enum_values);

                    if (!converted_field)
                    {
                        skip_tuple_value = true;
                        break;
                    }

                    bool need_insert_null = params.transform_null_in && lhs_tuple_element_types[i]->isNullable();
                    if (converted_field->isNull() && !need_insert_null)
                    {
                        skip_tuple_value = true;
                        break;
                    }

                    converted_tuple.emplace_back(std::move(*converted_field));
                }

                if (!skip_tuple_value)
                    column->insert(converted_tuple);
            }

            ColumnsWithTypeAndName res(1);
            res[0].type = lhs_type;
            res[0].column = std::move(column);
            return res;
        }

        /// Generic single-key column (all cases except `Nullable(Tuple(...))`, e.g. T / Nullable(T) / Tuple() / Tuple(T))
        for (size_t collection_index = 0; collection_index < rhs_collection.size(); ++collection_index)
        {
            const auto & rhs_element = rhs_collection[collection_index];

            auto field
                = convertFieldToTypeCheckEnum(rhs_element, *rhs_types[collection_index], *lhs_type, params.forbid_unknown_enum_values);

            bool need_insert_null = params.transform_null_in && column->isNullable();
            if (field && (!field->isNull() || need_insert_null))
                column->insert(*field);
        }

        ColumnsWithTypeAndName res(1);
        res[0].type = lhs_type;
        res[0].column = std::move(column);
        return res;
    }

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
            continue;
        }

        if (rhs_element.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type in set. Expected tuple, got {}", rhs_element.getTypeName());

        const auto & rhs_element_as_tuple = rhs_element.template safeGet<Tuple>();

        const DataTypePtr & rhs_element_type = rhs_types[collection_index];
        const DataTypeTuple * tuple_type = getTupleType(rhs_element_type);

        if (!tuple_type)
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                "Invalid element type in set. Expected Tuple, got {}", rhs_element_type->getName());

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
        }
    }

    ColumnsWithTypeAndName res(num_elements);
    for (size_t i = 0; i < num_elements; ++i)
    {
        res[i].type = lhs_unpacked_types[i];
        res[i].column = std::move(columns[i]);
    }

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

    /// Unpack `Tuple(...)` into tuple elements.
    /// For `Nullable(Tuple(...))` we keep it as a single value and handle it in createBlockFromCollection() fast-path.
    bool lhs_is_tuple = false;
    const auto * lhs_nullable_type = typeid_cast<const DataTypeNullable *>(lhs_expression_type.get());
    const auto * lhs_tuple_type
        = typeid_cast<const DataTypeTuple *>(lhs_nullable_type ? lhs_nullable_type->getNestedType().get() : lhs_expression_type.get());

    if (lhs_tuple_type)
    {
        lhs_is_tuple = true;

        /// Do not unpack empty tuple or single element tuple.
        /// Do not unpack `Nullable(Tuple(...))` because in the end we build a single `Nullable(Tuple(...))` column anyway.
        if (!lhs_nullable_type && lhs_tuple_type->getElements().size() > 1)
            lhs_unpacked_types = lhs_tuple_type->getElements();
    }

    bool lhs_is_nullable = (lhs_nullable_type != nullptr);

    for (auto & lhs_element_type : lhs_unpacked_types)
    {
        if (const auto * set_element_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(lhs_element_type.get()))
            lhs_element_type = set_element_low_cardinality_type->getDictionaryType();
    }

    /// If we didn't unpack `Nullable(Tuple(...))`, we still need to remove `LowCardinality` from tuple elements
    /// to match the behavior of the unpacked path.
    if (lhs_tuple_type && lhs_nullable_type && lhs_tuple_type->getElements().size() > 1)
    {
        DataTypes nested_tuple_element_types = lhs_tuple_type->getElements();
        for (auto & element_type : nested_tuple_element_types)
        {
            if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(element_type.get()))
                element_type = low_cardinality_type->getDictionaryType();
        }

        DataTypePtr nested_tuple_type = std::make_shared<DataTypeTuple>(nested_tuple_element_types);
        lhs_unpacked_types = {std::make_shared<DataTypeNullable>(nested_tuple_type)};
    }

    auto build_from_array = [&](const Field & rhs_value, const DataTypePtr & type)
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        const auto & rhs_array = rhs_value.safeGet<Array>();
        DataTypes rhs_types(rhs_array.size(), array_type->getNestedType());
        return createBlockFromCollection(rhs_array, rhs_types, lhs_unpacked_types, params);
    };

    auto build_from_tuple = [&](const Field & rhs_value, const DataTypePtr & type)
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        const auto & rhs_tuple = rhs_value.safeGet<Tuple>();
        const auto & rhs_types = tuple_type->getElements();
        return createBlockFromCollection(rhs_tuple, rhs_types, lhs_unpacked_types, params);
    };

    auto build_from_single_value = [&](const Field & rhs_value, const DataTypePtr & type)
    {
        /// Maybe we can inline without this lambda? But this way it's more readable.
        return createBlockFromCollection(Array{rhs_value}, DataTypes{type}, lhs_unpacked_types, params);
    };

    auto append_set_elements = [](ColumnsWithTypeAndName & destination, const ColumnsWithTypeAndName & source)
    {
        chassert(destination.size() == source.size());

        for (size_t i = 0; i < destination.size(); ++i)
        {
            chassert(source[i].column);
            chassert(destination[i].column);

            if (source[i].column->empty())
                continue;

            if (destination[i].column->empty())
            {
                destination[i].column = source[i].column;
                continue;
            }

            MutableColumnPtr merged = IColumn::mutate(std::move(destination[i].column));
            merged->reserve(merged->size() + source[i].column->size());
            merged->insertRangeFrom(*source[i].column, 0, source[i].column->size());
            destination[i].column = std::move(merged);
        }
    };


    size_t lhs_type_depth = getCompoundTypeDepth(*lhs_expression_type);
    size_t rhs_type_depth = getCompoundTypeDepth(*rhs_type);

    /// CAST(NULL, `Nullable(Tuple(...))`) IN NULL
    if (lhs_type_depth == rhs_type_depth + 1)
    {
        if (rhs.isNull())
            return build_from_single_value(rhs, rhs_type);
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
            return build_from_array(rhs, rhs_type);
        }

        if (lhs_is_tuple && rhs_which_type.isTuple() && is_null_in_rhs && (lhs_is_nullable || has_tuple_in_rhs))
        {
            /// RHS tuple can represent either:
            /// - a set of elements (NULLs and/or tuples): (NULL, NULL, (1, 2), ...)
            /// - a tuple literal (not a set): (NULL, 42) for `Tuple(Nullable(...), ...)`
            ///
            /// Examples:
            /// - CAST(NULL, `Nullable(Tuple(...))`) IN (NULL, NULL, (1, 2), ...)
            /// - Tuple(...) IN (NULL, NULL, (1, 2), ...)
            /// - Nullable(Tuple(...)) IN (NULL, 42) SETTINGS transform_null_in = 1
            ///
            /// If RHS contains a non-null non-tuple element, it cannot be a set of tuples
            const auto & rhs_tuple = rhs.safeGet<Tuple>();

            bool rhs_tuple_all_null = true;
            bool rhs_tuple_has_non_null_non_tuple = false;
            for (const auto & elem : rhs_tuple)
            {
                if (elem.isNull())
                    continue;

                rhs_tuple_all_null = false;

                if (elem.getType() != Field::Types::Tuple)
                {
                    rhs_tuple_has_non_null_non_tuple = true;
                    break;
                }
            }

            /// Treat as a set of elements:
            /// - Tuple(...) IN (NULL, (1, 2), (3, 4))
            /// - CAST(NULL, `Nullable(Tuple(...))`) IN (NULL, NULL, (1, 2), ...)
            if (!rhs_tuple_has_non_null_non_tuple)
            {
                auto res = build_from_tuple(rhs, rhs_type);

                /// Additionally, for `Nullable(Tuple(...)) IN (NULL, NULL)` also treat RHS as a tuple literal `(NULL, NULL)`
                /// when it can be cast to the LHS tuple type (so both interpretations are supported)
                /// Example: tuple(NULL, NULL)::Nullable(Tuple(Nullable(UInt32), Nullable(UInt32))) IN (NULL, NULL) SETTINGS transform_null_in = 1
                /// For `Nullable(Tuple(...)) IN (NULL, NULL)` also add a tuple-literal interpretation `(NULL, NULL)` when possible
                if (lhs_is_nullable && rhs_tuple_all_null && rhs_tuple.size() == lhs_tuple_type->getElements().size())
                {
                    /// Tuple literal `(NULL, NULL)` is representable only if all tuple elements are Nullable,
                    /// otherwise it would require NULL -> non-nullable conversion (e.g. `Nullable(Tuple(Int64, Int64))`).
                    bool all_tuple_elements_nullable = params.transform_null_in;
                    for (const auto & element_type : lhs_tuple_type->getElements())
                    {
                        if (!element_type->isNullable())
                        {
                            all_tuple_elements_nullable = false;
                            break;
                        }
                    }

                    /// `res` already contains the "set-of-elements" interpretation of RHS:
                    /// (NULL, NULL) -> {NULL, NULL}
                    /// If tuple literal `(NULL, NULL)` is representable for the LHS type, also add it:
                    /// {NULL, NULL, (NULL, NULL)}
                    if (all_tuple_elements_nullable)
                        append_set_elements(res, build_from_single_value(rhs, rhs_type));
                }

                return res;
            }
        }

        /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
        return build_from_single_value(rhs, rhs_type);
    }
    else if (lhs_type_depth + 1 == rhs_type_depth)
    {
        /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4))
        WhichDataType rhs_which_type(rhs_type);

        if (rhs_which_type.isArray())
            return build_from_array(rhs, rhs_type);

        if (rhs_which_type.isTuple())
            return build_from_tuple(rhs, rhs_type);

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
