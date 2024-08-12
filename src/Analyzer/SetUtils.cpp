#include <memory>
#include <cassert>
#include <vector>
#include <Analyzer/SetUtils.h>

#include <Core/Block.h>

#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <Core/TypeId.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_ELEMENT_OF_SET;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
            if (!tuple_elements.empty())
                current_type = tuple_elements.at(0).get();
            else
            {
                /// Special case: tuple with no element - tuple(). In this case, what's the compound type depth?
                /// I'm not certain about the theoretical answer, but from experiment, 1 is the most reasonable choice.
                return 1;
            }

            ++result;
        }
        else
        {
            break;
        }
    }

    return result;
}

/** Check whether the Tuple elements types (value_types here) meet the requirements of IN Operator and obtain the types of Block to be constructed.
  * This function is executed according to the following logic:
  * - if transform_null_in == false, just use left arg's types as Block type. (the Tuple elements types will be checked in createBlockFromCollection)
  * - distinguish situations where the number of left arg is one or more:
  *   - one : xxx IN (xxx, xxx, xxx, ...)
  *     * if left arg's type is not NULL, just use it as Block type.
  *     * select the first non-null-type in Tuple as the type of Block, and count whether there is a NULL type in Tuple.
  *     * if such a type does not exist, it means that all types in Tuple are NULL, then select NULL as the type of Block.
  *     * if the selected type is not a Nullable type and there is a NULL type in the Tuple, the selected type will be converted to the corresponding Nullable type. like:
  *       NULL in (1, 2, NULL), we need to choose Nullable(UInt8) as Block type.
  *     * if the selected type does not have a corresponding Nullable type, an exception is thrown.
  *   - more : (xxx, yyy, ...) IN ((xxx, yyy, ...), ...)
  *     extract the types from Tuple one by one, handle them according to the first situation, and finally merge them back into Tuple.
  */
DataTypes CheckAndGetBlockTypes(const DataTypes & block_types, const DataTypes & value_types, bool transform_null_in)
{
    if (transform_null_in == false)
    {
        return block_types;
    }
    DataTypes result;
    size_t columns_size = block_types.size();
    if (columns_size == 1)
    {
        if (!block_types[0]->isNullableNothing())
        {
            result.push_back(block_types[0]);
        }
        else
        {
            bool value_types_has_null_literal = false;
            for (const auto & value_type : value_types)
            {
                // select the first not-nullable-nothing type in set as block type.
                if (!value_type->isNullableNothing())
                {
                    if (result.empty())
                        result.push_back(value_type);
                }
                else
                {
                    value_types_has_null_literal = true;
                }
            }
            // e.g. NULL in (NULL, NULL, NULL)
            if (result.empty())
            {
                assert(value_types_has_null_literal == true);
                result.push_back(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()));
            }
            // e.g. NULL in (1, 2, NULL), select UInt8 as block type now.
            // But this is not correct because we will insert NULL into the UInt8 type column later
            if (value_types_has_null_literal == true && !result[0]->isNullable())
            {
                if (!result[0]->canBeInsideNullable())
                {
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "DataType {} can not be inside nullable with NULL set element", result[0]->getName());
                }
                result[0] = std::make_shared<DataTypeNullable>(result[0]);
            }
        }
    }
    else
    {
        std::vector<DataTypes> value_tuple_types(columns_size);
        for (const auto & value_type : value_types)
        {
            if (value_type->getTypeId() != TypeIndex::Tuple)
            {
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                    "Invalid type in set. Expected tuple, got {}",
                    value_type->getName());
            }
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(value_type.get());
            const auto & tuple_element_types = tuple_type->getElements();
            if (tuple_element_types.size() != columns_size)
            {
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                    "Incorrect size of tuple in set: {} instead of {}",
                    tuple_element_types.size(),
                    columns_size);
            }
            for (size_t i = 0; i < columns_size; ++i)
            {
                value_tuple_types[i].push_back(tuple_element_types[i]);
            }
        }
        for (size_t i = 0; i < columns_size; ++i)
        {
            DataTypes tuple_element_block_type = CheckAndGetBlockTypes({block_types[i]}, value_tuple_types[i], transform_null_in);
            result.push_back(tuple_element_block_type[0]);
        }
    }
    return result;
}

template <typename Collection>
Block createBlockFromCollection(const Collection & collection, const DataTypes& value_types, const DataTypes & block_types, bool transform_null_in)
{
    assert(collection.size() == value_types.size());
    size_t columns_size = block_types.size();
    MutableColumns columns(columns_size);

    DataTypes ret_types = CheckAndGetBlockTypes(block_types, value_types, transform_null_in);
    for (size_t i = 0; i < columns_size; ++i)
    {
        columns[i] = ret_types[i]->createColumn();
        columns[i]->reserve(columns_size);
    }

    Row tuple_values;

    for (size_t collection_index = 0; collection_index < collection.size(); ++collection_index)
    {
        const auto & value = collection[collection_index];
        if (columns_size == 1)
        {
            const DataTypePtr & data_type = value_types[collection_index];
            auto field = convertFieldToTypeStrictWithTransformNullIn(value, *data_type, *block_types[0], transform_null_in);
            if (!field)
            {
                continue;
            }

            bool need_insert_null = transform_null_in && block_types[0]->isNullable();
            if (!field->isNull() || need_insert_null)
                columns[0]->insert(*field);

            continue;
        }

        if (value.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                "Invalid type in set. Expected tuple, got {}",
                value.getTypeName());

        const auto & tuple = value.template get<const Tuple &>();
        const DataTypePtr & value_type = value_types[collection_index];
        const DataTypes & tuple_value_type = typeid_cast<const DataTypeTuple *>(value_type.get())->getElements();

        size_t tuple_size = tuple.size();

        if (tuple_size != columns_size)
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                "Incorrect size of tuple in set: {} instead of {}",
                tuple_size,
                columns_size);

        if (tuple_values.empty())
            tuple_values.resize(tuple_size);

        size_t i = 0;
        for (; i < tuple_size; ++i)
        {
            auto converted_field = convertFieldToTypeStrictWithTransformNullIn(tuple[i], *tuple_value_type[i], *block_types[i], transform_null_in);
            if (!converted_field)
                break;
            tuple_values[i] = std::move(*converted_field);

            bool need_insert_null = transform_null_in && block_types[i]->isNullable();
            if (tuple_values[i].isNull() && !need_insert_null)
                break;
        }

        if (i == tuple_size)
            for (i = 0; i < tuple_size; ++i)
                columns[i]->insert(tuple_values[i]);
    }

    Block res;
    for (size_t i = 0; i < columns_size; ++i)
        res.insert(ColumnWithTypeAndName{std::move(columns[i]), ret_types[i], "argument_" + toString(i)});

    return res;
}

}

Block getSetElementsForConstantValue(const DataTypePtr & expression_type, const Field & value, const DataTypePtr & value_type, bool transform_null_in, bool & expression_type_has_nullable_nothing)
{
    DataTypes set_element_types = {expression_type};
    const auto * lhs_tuple_type = typeid_cast<const DataTypeTuple *>(expression_type.get());

    if (lhs_tuple_type && lhs_tuple_type->getElements().size() != 1)
        set_element_types = lhs_tuple_type->getElements();

    for (auto & set_element_type : set_element_types)
    {
        if (const auto * set_element_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(set_element_type.get()))
            set_element_type = set_element_low_cardinality_type->getDictionaryType();
        if (set_element_type->isNullableNothing())
        {
            expression_type_has_nullable_nothing = true;
        }
    }

    size_t lhs_type_depth = getCompoundTypeDepth(*expression_type);
    size_t rhs_type_depth = getCompoundTypeDepth(*value_type);

    Block result_block;

    if (lhs_type_depth == rhs_type_depth)
    {
        /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
        Array array{value};
        DataTypes value_types{value_type};
        result_block = createBlockFromCollection(array, value_types, set_element_types, transform_null_in);
    }
    else if (lhs_type_depth + 1 == rhs_type_depth)
    {
        /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4))
        WhichDataType rhs_which_type(value_type);

        if (rhs_which_type.isArray())
        {
            const DataTypeArray * value_array_type = assert_cast<const DataTypeArray *>(value_type.get());
            size_t value_array_size = value.get<const Array &>().size();
            DataTypes value_types(value_array_size, value_array_type->getNestedType());
            result_block = createBlockFromCollection(value.get<const Array &>(), value_types, set_element_types, transform_null_in);
        }
        else if (rhs_which_type.isTuple())
        {
            const DataTypeTuple * value_tuple_type = assert_cast<const DataTypeTuple *>(value_type.get());
            const DataTypes & value_types = value_tuple_type->getElements();
            result_block = createBlockFromCollection(value.get<const Tuple &>(), value_types, set_element_types, transform_null_in);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Unsupported type at the right-side of IN. Expected Array or Tuple. Actual {}",
                value_type->getName());
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported types for IN. First argument type {}. Second argument type {}",
            expression_type->getName(),
            value_type->getName());
    }

    return result_block;
}

}
