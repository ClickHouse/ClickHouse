#include "ParquetBloomFilterCondition.h"
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
        assert(size <= buffer_size);
        std::copy(view.begin(), view.end(), buffer);
        return parquet::ByteArray(size, buffer);
    }
}

bool maybeTrueOnBloomFilter(const IColumn * data_column, const std::unique_ptr<parquet::BloomFilter> & bloom_filter, bool match_all)
{
    static constexpr uint32_t buffer_size = 32;
    uint8_t buffer[buffer_size] = {0};

    for (size_t i = 0; i < data_column->size(); ++i)
    {
        const auto data_view = data_column->getDataAt(i).toView();

        const auto ba = createByteArray(data_view, data_column->getDataType(), buffer, buffer_size);
        bool found = bloom_filter->FindHash(bloom_filter->Hash(&ba));

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
    const DB::ActionsDAGPtr & filter_actions_dag, DB::ContextPtr context_, const DB::Block & header_)
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
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
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
                 || element.function == RPNElement::FUNCTION_NOT_EQUALS)
        {
            bool match_rows = true;
            bool match_all = element.function == RPNElement::FUNCTION_HAS_ALL;
            const auto & predicate = element.predicate;
            for (size_t index = 0; match_rows && index < predicate.size(); ++index)
            {
                const auto & query_index_hash = predicate[index];

                if (column_index_to_bf.contains(query_index_hash.first))
                {
                    const auto & filter = column_index_to_bf.at(query_index_hash.first);
                    const ColumnPtr & hash_column = query_index_hash.second;

                    match_rows = maybeTrueOnBloomFilter(&*hash_column,
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

bool ParquetBloomFilterCondition::extractAtomFromTree(const RPNBuilderTreeNode & node, ParquetBloomFilterCondition::RPNElement & out)
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

    return traverseFunction(node, out, nullptr /*parent*/);
}

bool ParquetBloomFilterCondition::traverseFunction(
    const RPNBuilderTreeNode & node, ParquetBloomFilterCondition::RPNElement & out, const RPNBuilderTreeNode * parent)
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
            if (traverseFunction(argument, out, &node))
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
                        [[maybe_unused]] const auto prepared_info = getPreparedSetInfo(prepared_set);
//                        if (traverseTreeIn(function_name, lhs_argument, prepared_set, prepared_info.type, prepared_info.column, out))
                            maybe_useful = true;
                    }
                }
            }
        }
        else if (function_name == "equals" || function_name == "notEquals")
        {
            Field const_value;
            DataTypePtr const_type;

            if (rhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseTreeEquals(function_name, lhs_argument, const_type, const_value, out, parent))
                    maybe_useful = true;
            }
            else if (lhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseTreeEquals(function_name, rhs_argument, const_type, const_value, out, parent))
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
    ParquetBloomFilterCondition::RPNElement & out,
    const RPNBuilderTreeNode *)
{
    auto key_column_name = key_node.getColumnName();

    if (!header.has(key_column_name))
    {
        return false;
    }

    size_t position = header.getPositionByName(key_column_name);
    const DataTypePtr & index_type = header.getByPosition(position).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

    if (array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "An array type of bloom_filter supports only has(), indexOf(), and hasAny() functions.");

    out.function = function_name == "equals" ? RPNElement::FUNCTION_EQUALS : RPNElement::FUNCTION_NOT_EQUALS;
    const DataTypePtr actual_type = getPrimitiveType(index_type);
    auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
    if (converted_field.isNull())
        return false;

    //            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
    auto column = actual_type->createColumn();
    column->insert(converted_field);
    out.predicate.emplace_back(position, std::move(column));

    return true;
}

}
