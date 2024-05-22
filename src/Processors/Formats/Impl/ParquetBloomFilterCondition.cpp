#include "ParquetBloomFilterCondition.h"

#if USE_PARQUET

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/misc.h>
#include <Interpreters/convertFieldToType.h>
#include <Columns/ColumnConst.h>

#include <parquet/bloom_filter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

parquet::ByteArray createByteArray(std::string_view view, TypeIndex type, uint8_t * buffer, uint32_t buffer_size)
{
    if (isStringOrFixedString(type))
    {
        return view;
    }
    else
    {
        auto size = static_cast<uint32_t>(std::max(view.size(), sizeof(uint32_t)));
        chassert(size <= buffer_size);
        std::copy(view.begin(), view.end(), buffer);
        return parquet::ByteArray(size, buffer);
    }
}

ColumnPtr hash(const auto & data_column, const std::unique_ptr<parquet::BloomFilter> & bloom_filter)
{
    static constexpr uint32_t buffer_size = 32;
    uint8_t buffer[buffer_size] = {0};

    auto column_size = data_column->size();

    auto hashes_column = ColumnUInt64::create(column_size);
    ColumnUInt64::Container & hashes_internal_data = hashes_column->getData();

    for (size_t i = 0; i < column_size; ++i)
    {
        const auto data_view = data_column->getDataAt(i).toView();

        const auto ba = createByteArray(data_view, data_column->getDataType(), buffer, buffer_size);

        const auto hash = bloom_filter->Hash(&ba);

        hashes_internal_data[i] = hash;
    }

    return hashes_column;
}

bool maybeTrueOnBloomFilter(ColumnPtr hash_column, const std::unique_ptr<parquet::BloomFilter> & bloom_filter, bool match_all)
{
    const auto * uint64_column = typeid_cast<const ColumnUInt64 *>(hash_column.get());

    if (!uint64_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash column must be UInt64.");

    const ColumnUInt64::Container & hashes = uint64_column->getData();

    for (const auto hash : hashes)
    {
        bool found = bloom_filter->FindHash(hash);

        if (match_all && !found)
            return false;
        if (!match_all && found)
            return true;
    }

    return match_all;
}

DataTypePtr getPrimitiveType(const DataTypePtr & data_type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        if (!typeid_cast<const DataTypeArray *>(array_type->getNestedType().get()))
            return getPrimitiveType(array_type->getNestedType());
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of bloom filter index.", data_type->getName());
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(data_type.get()))
        return getPrimitiveType(nullable_type->getNestedType());

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_type.get()))
        return getPrimitiveType(low_cardinality_type->getDictionaryType());

    return data_type;
}

ColumnWithTypeAndName getPreparedSetInfo(const ConstSetPtr & prepared_set)
{
    if (prepared_set->getDataTypes().size() == 1)
        return {prepared_set->getSetElements()[0], prepared_set->getElementsTypes()[0], "dummy"};

    Columns set_elements;
    for (auto & set_element : prepared_set->getSetElements())

        set_elements.emplace_back(set_element->convertToFullColumnIfConst());

    return {ColumnTuple::create(set_elements), std::make_shared<DataTypeTuple>(prepared_set->getElementsTypes()), "dummy"};
}

}

ParquetBloomFilterCondition::ParquetBloomFilterCondition(
    const DB::ActionsDAGPtr & filter_actions_dag,
    const IndexToColumnBF & index_to_column_hasher,
    DB::ContextPtr context_,
    const DB::Block & header_)
: header(header_)
{
    if (!filter_actions_dag)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        filter_actions_dag->getOutputs().at(0),
        context_,
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, index_to_column_hasher, out); });
    rpn = std::move(builder).extractRPN();
}

bool ParquetBloomFilterCondition::mayBeTrueOnRowGroup(const IndexToColumnBF & column_index_to_bf)
{
    std::vector<BoolMask> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
                 || element.function == RPNElement::FUNCTION_NOT_EQUALS
                 || element.function == RPNElement::FUNCTION_HAS
                 || element.function == RPNElement::FUNCTION_HAS_ANY
                 || element.function == RPNElement::FUNCTION_HAS_ALL
                 || element.function == RPNElement::FUNCTION_IN
                 || element.function == RPNElement::FUNCTION_NOT_IN
                 || element.function == RPNElement::ALWAYS_FALSE)
        {
            bool match_rows = true;
            bool match_all = element.function == RPNElement::FUNCTION_HAS_ALL;
            const auto & predicate = element.predicate;
            for (size_t index = 0; match_rows && index < predicate.size(); ++index)
            {
                const auto [column_index, column_ptr] = predicate[index];

                if (column_index_to_bf.contains(column_index))
                {
                    const auto & filter = column_index_to_bf.at(column_index);

                    match_rows = maybeTrueOnBloomFilter(column_ptr,
                                                        filter,
                                                        match_all);
                }
            }

            rpn_stack.emplace_back(match_rows, true);
            if (element.function == RPNElement::FUNCTION_NOT_EQUALS || element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::mayBeTrueInRange");

    return rpn_stack[0].can_be_true;
}

bool ParquetBloomFilterCondition::extractAtomFromTree(
    const RPNBuilderTreeNode & node,
    const IndexToColumnBF & index_to_column_hasher,
    ParquetBloomFilterCondition::RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
            if (const_value.getType() == Field::Types::UInt64)
            {
                out.function = const_value.get<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Int64)
            {
                out.function = const_value.get<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = const_value.get<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }
        }
    }

    return traverseFunction(node, index_to_column_hasher, out);
}

bool ParquetBloomFilterCondition::traverseTreeIn(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const ConstSetPtr &,
    const DataTypePtr & type,
    const ColumnPtr & column,
    const IndexToColumnBF & index_to_column_hasher,
    ParquetBloomFilterCondition::RPNElement & out)
{
    auto key_node_column_name = key_node.getColumnName();

    if (header.has(key_node_column_name))
    {
        size_t position = header.getPositionByName(key_node_column_name);

        if (!index_to_column_hasher.contains(position))
        {
            return false;
        }

        out.predicate.emplace_back(std::make_pair(position, hash(column, index_to_column_hasher.at(position))));

        if (function_name == "in"  || function_name == "globalIn")
            out.function = RPNElement::FUNCTION_IN;

        if (function_name == "notIn"  || function_name == "globalNotIn")
            out.function = RPNElement::FUNCTION_NOT_IN;

        return true;
    }

    if (key_node.isFunction())
    {
        auto key_node_function = key_node.toFunctionNode();
        auto key_node_function_name = key_node_function.getFunctionName();
        size_t key_node_function_arguments_size = key_node_function.getArgumentsSize();

        WhichDataType which(type);

        if (which.isTuple() && key_node_function_name == "tuple")
        {
            const auto & tuple_column = typeid_cast<const ColumnTuple *>(column.get());
            const auto & tuple_data_type = typeid_cast<const DataTypeTuple *>(type.get());

            if (tuple_data_type->getElements().size() != key_node_function_arguments_size || tuple_column->getColumns().size() != key_node_function_arguments_size)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types of arguments of function {}", function_name);

            bool match_with_subtype = false;
            const auto & sub_columns = tuple_column->getColumns();
            const auto & sub_data_types = tuple_data_type->getElements();

            for (size_t index = 0; index < key_node_function_arguments_size; ++index)
                match_with_subtype |= traverseTreeIn(function_name, key_node_function.getArgumentAt(index), nullptr, sub_data_types[index], sub_columns[index], index_to_column_hasher, out);

            return match_with_subtype;
        }
    }

    return false;
}

bool ParquetBloomFilterCondition::traverseFunction(
    const RPNBuilderTreeNode & node,
    const IndexToColumnBF & index_to_column_hasher,
    ParquetBloomFilterCondition::RPNElement & out)
{
    bool maybe_useful = false;

    if (node.isFunction())
    {
        const auto function = node.toFunctionNode();
        auto arguments_size = function.getArgumentsSize();
        auto function_name = function.getFunctionName();

        for (size_t i = 0; i < arguments_size; ++i)
        {
            auto argument = function.getArgumentAt(i);
            if (traverseFunction(argument, index_to_column_hasher, out))
                maybe_useful = true;
        }

        if (arguments_size != 2)
            return false;

        auto lhs_argument = function.getArgumentAt(0);
        auto rhs_argument = function.getArgumentAt(1);

        if (functionIsInOrGlobalInOperator(function_name))
        {
            if (auto future_set = rhs_argument.tryGetPreparedSet(); future_set)
            {
                if (auto prepared_set = future_set->buildOrderedSetInplace(rhs_argument.getTreeContext().getQueryContext()); prepared_set)
                {
                    if (prepared_set->hasExplicitSetElements())
                    {
                        const auto prepared_info = getPreparedSetInfo(prepared_set);
                        if (traverseTreeIn(function_name, lhs_argument, prepared_set, prepared_info.type, prepared_info.column, index_to_column_hasher, out))
                            maybe_useful = true;
                    }
                }
            }
        }
        else if (function_name == "equals" ||
                 function_name == "notEquals" ||
                 function_name == "has" ||
                 function_name == "hasAny" ||
                 function_name == "hasAll")
        {
            Field const_value;
            DataTypePtr const_type;

            if (rhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseTreeEquals(function_name, lhs_argument, const_type, const_value, index_to_column_hasher, out))
                    maybe_useful = true;
            }
            else if (lhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseTreeEquals(function_name, rhs_argument, const_type, const_value, index_to_column_hasher, out))
                    maybe_useful = true;
            }
        }
    }

    return maybe_useful;
}

bool ParquetBloomFilterCondition::traverseTreeEquals(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    const IndexToColumnBF & index_to_column_hasher,
    ParquetBloomFilterCondition::RPNElement & out)
{
    auto key_column_name = key_node.getColumnName();

    if (!header.has(key_column_name))
    {
        return false;
    }

    size_t position = header.getPositionByName(key_column_name);
    const DataTypePtr & index_type = header.getByPosition(position).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

    if (function_name == "has")
    {
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an array.", function_name);

        out.function = RPNElement::FUNCTION_HAS;
        const DataTypePtr actual_type = getPrimitiveType(array_type->getNestedType());
        auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
        if (converted_field.isNull())
            return false;

        auto column = actual_type->createColumn();
        column->insert(converted_field);

        if (!index_to_column_hasher.contains(position))
        {
            return false;
        }

        out.predicate.emplace_back(position, hash(column, index_to_column_hasher.at(position)));
    }
    else if (function_name == "hasAny" || function_name == "hasAll")
    {
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an array.", function_name);

        if (value_field.getType() != Field::Types::Array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be an array.", function_name);

        const DataTypePtr actual_type = getPrimitiveType(array_type->getNestedType());
        ColumnPtr column;

        {
            const bool is_nullable = actual_type->isNullable();
            auto mutable_column = actual_type->createColumn();

            for (const auto & f : value_field.get<Array>())
            {
                if ((f.isNull() && !is_nullable) || f.isDecimal(f.getType())) /// NOLINT(readability-static-accessed-through-instance)
                    return false;

                auto converted = convertFieldToType(f, *actual_type);
                if (converted.isNull())
                    return false;

                mutable_column->insert(converted);
            }

            column = std::move(mutable_column);
        }

        out.function = function_name == "hasAny" ?
                                                 RPNElement::FUNCTION_HAS_ANY :
                                                 RPNElement::FUNCTION_HAS_ALL;

        if (!index_to_column_hasher.contains(position))
        {
            return false;
        }

        out.predicate.emplace_back(std::make_pair(position, hash(column, index_to_column_hasher.at(position))));
    }
    else
    {
        if (array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "An array type of bloom_filter supports only has() and hasAny() functions.");

        out.function = function_name == "equals" ? RPNElement::FUNCTION_EQUALS : RPNElement::FUNCTION_NOT_EQUALS;
        const DataTypePtr actual_type = getPrimitiveType(index_type);
        auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
        if (converted_field.isNull())
            return false;

        auto column = actual_type->createColumn();
        column->insert(converted_field);

        if (!index_to_column_hasher.contains(position))
        {
            return false;
        }

        out.predicate.emplace_back(position, hash(column, index_to_column_hasher.at(position)));
    }

    return true;
}

}

#endif
