#include <Analyzer/SetUtils.h>

#include <Core/Block.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>

#include <Common/assert_cast.h>

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

template <typename Collection>
Block createBlockFromCollection(const Collection & collection, const DataTypes& value_types, const DataTypes & block_types, bool transform_null_in)
{
    assert(collection.size() == value_types.size());
    size_t columns_size = block_types.size();
    MutableColumns columns(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
    {
        columns[i] = block_types[i]->createColumn();
        columns[i]->reserve(collection.size());
    }

    Row tuple_values;

    for (size_t collection_index = 0; collection_index < collection.size(); ++collection_index)
    {
        const auto & value = collection[collection_index];
        if (columns_size == 1)
        {
            const DataTypePtr & data_type = value_types[collection_index];
            auto field = convertFieldToTypeStrict(value, *data_type, *block_types[0]);
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

        const auto & tuple = value.template safeGet<const Tuple &>();
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
            auto converted_field = convertFieldToTypeStrict(tuple[i], *tuple_value_type[i], *block_types[i]);
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
        res.insert(ColumnWithTypeAndName{std::move(columns[i]), block_types[i], "argument_" + toString(i)});

    return res;
}

}

Block getSetElementsForConstantValue(const DataTypePtr & expression_type, const Field & value, const DataTypePtr & value_type, bool transform_null_in)
{
    DataTypes set_element_types = {expression_type};
    const auto * lhs_tuple_type = typeid_cast<const DataTypeTuple *>(expression_type.get());

    if (lhs_tuple_type && lhs_tuple_type->getElements().size() != 1)
        set_element_types = lhs_tuple_type->getElements();

    for (auto & set_element_type : set_element_types)
    {
        if (const auto * set_element_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(set_element_type.get()))
            set_element_type = set_element_low_cardinality_type->getDictionaryType();
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
            size_t value_array_size = value.safeGet<const Array &>().size();
            DataTypes value_types(value_array_size, value_array_type->getNestedType());
            result_block = createBlockFromCollection(value.safeGet<const Array &>(), value_types, set_element_types, transform_null_in);
        }
        else if (rhs_which_type.isTuple())
        {
            const DataTypeTuple * value_tuple_type = assert_cast<const DataTypeTuple *>(value_type.get());
            const DataTypes & value_types = value_tuple_type->getElements();
            result_block = createBlockFromCollection(value.safeGet<const Tuple &>(), value_types, set_element_types, transform_null_in);
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
