#include <Analyzer/SetUtils.h>

#include <Core/Block.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
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

size_t getCompoundTypeDepth(const IDataType & type)
{
    size_t result = 0;

    const IDataType * current_type = &type;

    while (true)
    {
        WhichDataType which_type(*current_type);

        if (which_type.isArray())
        {
            current_type = assert_cast<const DataTypeArray &>(*current_type).getNestedType().get();
            ++result;
        }
        else if (which_type.isTuple())
        {
            const auto & tuple_elements = assert_cast<const DataTypeTuple &>(*current_type).getElements();
            ++result;
            if (!tuple_elements.empty())
                current_type = tuple_elements.at(0).get();
            else
            {
                break;
            }
        }
        else
        {
            break;
        }
    }

    return result;
}

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
/// Array: [1, 2, 3], [(1, 2), (3, 4)]
/// Tuple: (1, 2, 3), ((1, 2), (3, 4), (5, 6), (7, 8))
template <typename Collection>
ColumnsWithTypeAndName createBlockFromCollection(
    const Collection & rhs_collection, const DataTypes & rhs_types, const DataTypes & lhs_unpacked_types, GetSetElementParams params)
{
    assert(rhs_collection.size() == rhs_types.size());
    size_t columns_size = lhs_unpacked_types.size();
    MutableColumns columns(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
    {
        columns[i] = lhs_unpacked_types[i]->createColumn();
        columns[i]->reserve(rhs_collection.size());
    }

    Row converted_rhs_element_unpacked;

    for (size_t collection_index = 0; collection_index < rhs_collection.size(); ++collection_index)
    {
        const auto & rhs_element = rhs_collection[collection_index];
        if (columns_size == 1)
        {
            const DataTypePtr & data_type = rhs_types[collection_index];
            auto field = convertFieldToTypeCheckEnum(rhs_element, *data_type, *lhs_unpacked_types[0], params.forbid_unknown_enum_values);
            if (!field)
                continue;

            bool need_insert_null = params.transform_null_in && lhs_unpacked_types[0]->isNullable();
            if (!field->isNull() || need_insert_null)
                columns[0]->insert(*field);

            continue;
        }

        if (rhs_element.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type in set. Expected tuple, got {}", rhs_element.getTypeName());

        const auto & rhs_element_as_tuple = rhs_element.template safeGet<Tuple>();
        const DataTypePtr & rhs_element_type = rhs_types[collection_index];
        const DataTypes & rhs_element_unpacked_types = typeid_cast<const DataTypeTuple *>(rhs_element_type.get())->getElements();

        size_t tuple_size = rhs_element_as_tuple.size();

        if (tuple_size != columns_size)
            throw Exception(
                ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Incorrect size of tuple in set: {} instead of {}", tuple_size, columns_size);

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
            for (i = 0; i < tuple_size; ++i)
                columns[i]->insert(converted_rhs_element_unpacked[i]);
    }

    ColumnsWithTypeAndName res(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
    {
        res[i].type = lhs_unpacked_types[i];
        res[i].column = std::move(columns[i]);
    }

    return res;
}

}

/// lhs IN rhs
ColumnsWithTypeAndName getSetElementsForConstantValue(
    const DataTypePtr & lhs_expression_type, const Field & rhs, const DataTypePtr & rhs_type, GetSetElementParams params)
{
    const auto * lhs_tuple_type = typeid_cast<const DataTypeTuple *>(lhs_expression_type.get());

    DataTypes lhs_unpacked_types = {lhs_expression_type};

    /// Do not unpack if empty tuple or single element tuple
    if (lhs_tuple_type && lhs_tuple_type->getElements().size() > 1)
        lhs_unpacked_types = lhs_tuple_type->getElements();

    for (auto & lhs_element_type : lhs_unpacked_types)
    {
        if (const auto * set_element_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(lhs_element_type.get()))
            lhs_element_type = set_element_low_cardinality_type->getDictionaryType();
    }

    size_t lhs_type_depth = getCompoundTypeDepth(*lhs_expression_type);
    size_t rhs_type_depth = getCompoundTypeDepth(*rhs_type);

    ColumnsWithTypeAndName result_block;

    if (lhs_type_depth == rhs_type_depth)
    {
        /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
        Array rhs_array{rhs};
        DataTypes rhs_types{rhs_type};
        result_block = createBlockFromCollection(rhs_array, rhs_types, lhs_unpacked_types, params);
    }
    else if (lhs_type_depth + 1 == rhs_type_depth)
    {
        /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4))
        WhichDataType rhs_which_type(rhs_type);

        if (rhs_which_type.isArray())
        {
            const DataTypeArray * rhs_array_type = assert_cast<const DataTypeArray *>(rhs_type.get());
            size_t rhs_array_size = rhs.safeGet<Array>().size();
            DataTypes rhs_types(rhs_array_size, rhs_array_type->getNestedType());
            result_block = createBlockFromCollection(rhs.safeGet<Array>(), rhs_types, lhs_unpacked_types, params);
        }
        else if (rhs_which_type.isTuple())
        {
            const DataTypeTuple * rhs_tuple_type = assert_cast<const DataTypeTuple *>(rhs_type.get());
            const DataTypes & rhs_types = rhs_tuple_type->getElements();
            result_block = createBlockFromCollection(rhs.safeGet<Tuple>(), rhs_types, lhs_unpacked_types, params);
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Unsupported type at the right-side of IN. Expected Array or Tuple. Actual {}",
                rhs_type->getName());
    }
    else
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported types for IN. First argument type {}. Second argument type {}",
            lhs_expression_type->getName(),
            rhs_type->getName());
    }

    return result_block;
}

}
