#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Set.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilterFactory.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int NOT_IMPLEMENTED;
}

namespace DB
{
ColumnFilterFactory::~ColumnFilterFactory() = default;

namespace
{
bool isInputNode(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::INPUT;
}

bool hasInputNode(const ActionsDAG::Node & node)
{
    for (const auto & child : node.children)
    {
        if (isInputNode(*child))
            return true;
    }
    return false;
}

bool isFunctionNodeWithInput(const ActionsDAG::Node & node)
{
    return node.function_base != nullptr && hasInputNode(node);
}

bool isNotFunctionNode(const ActionsDAG::Node & node)
{
    return node.function_base && node.function_base->getName() == "not";
}

const ActionsDAG::Node * getFirstChildNode(const ActionsDAG::Node & node)
{
    return node.children.at(0);
}

bool isConstantNode(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::COLUMN;
}

bool isCompareColumnWithConst(const ActionsDAG::Node & node)
{
    if (!isFunctionNodeWithInput(node))
        return false;
    size_t input_count = 0;
    size_t constant_count = 0;
    for (const auto & child : node.children)
    {
        if (isInputNode(*child))
            ++input_count;
        if (isConstantNode(*child))
            ++constant_count;
    }
    return input_count == 1 && constant_count >= 1;
}

const ActionsDAG::Node * getInputNode(const ActionsDAG::Node & node)
{
    for (const auto & child : node.children)
    {
        if (isInputNode(*child))
            return child;
    }
    throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "No input node found in {}", node.result_name);
}

bool isIntInput(const ActionsDAG::Node & node)
{
    const auto * input_node = getInputNode(node);
    auto input_type = removeNullable(input_node->result_type);
    if (!isInt64(input_type) && !isInt32(input_type) && !isInt16(input_type)
        && !isDateOrDate32(input_type) && !isDateTimeOrDateTime64(input_type))
        return false;
    return true;
}

bool isStringInput(const ActionsDAG::Node & node)
{
    const auto * input_node = getInputNode(node);
    auto input_type = removeNullable(input_node->result_type);
    if (!isString(input_type))
        return false;
    return true;
}

ActionsDAG::NodeRawConstPtrs getConstantNode(const ActionsDAG::Node & node)
{
    ActionsDAG::NodeRawConstPtrs result;
    for (const auto & child : node.children)
    {
        if (isConstantNode(*child))
            result.push_back(child);
    }
    return result;
}
}

bool BigIntRangeFilterFactory::validate(const ActionsDAG::Node & node)
{
    if (!isCompareColumnWithConst(node))
        return false;
    if (!isIntInput(node))
        return false;
    static const std::unordered_set<String> supported_functions = {"equals", "less", "greater", "lessOrEquals", "greaterOrEquals"};
    if (!supported_functions.contains(node.function_base->getName()))
        return false;
    return true;
}

NamedColumnFilter BigIntRangeFilterFactory::create(const ActionsDAG::Node & node)
{
    const auto * input_node = getInputNode(node);
    auto name = input_node->result_name;
    auto constant_nodes = getConstantNode(node);
    auto func_name = node.function_base->getName();
    ColumnFilterPtr filter = nullptr;
    if (func_name == "equals")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        filter = std::make_shared<BigIntRangeFilter>(value, value, false);
    }
    else if (func_name == "less")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        filter = std::make_shared<BigIntRangeFilter>(std::numeric_limits<Int64>::min(), value - 1, false);
    }
    else if (func_name == "greater")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        filter = std::make_shared<BigIntRangeFilter>(value + 1, std::numeric_limits<Int64>::max(), false);
    }
    else if (func_name == "lessOrEquals")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        filter = std::make_shared<BigIntRangeFilter>(std::numeric_limits<Int64>::min(), value, true);
    }
    else if (func_name == "greaterOrEquals")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        filter = std::make_shared<BigIntRangeFilter>(value, std::numeric_limits<Int64>::max(), true);
    }
    if (filter)
    {
        return std::make_pair(name, filter);
    }
    throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported filter function {}", func_name);
}

bool NegatedBigIntRangeFilterFactory::validate(const ActionsDAG::Node & node)
{
    if (isCompareColumnWithConst(node) && node.function_base->getName() == "notEquals" && isIntInput(node))
        return true;
    if (!isNotFunctionNode(node))
        return false;
    const auto * child = getFirstChildNode(node);
    return non_negated_factory.validate(*child);
}

NamedColumnFilter NegatedBigIntRangeFilterFactory::create(const ActionsDAG::Node & node)
{
    auto func_name = node.function_base->getName();
    if (func_name == "notEquals")
    {
        const auto * input_node = getInputNode(node);
        auto name = input_node->result_name;
        auto constant_nodes = getConstantNode(node);
        Int64 value = constant_nodes.front()->column->getInt(0);
        auto filter = std::make_shared<NegatedBigIntRangeFilter>(value, value, false);
        return std::make_pair(name, filter);
    }
    const auto * child = getFirstChildNode(node);
    auto non_negated_filter = non_negated_factory.create(*child);
    BigIntRangeFilter * range_filter = dynamic_cast<BigIntRangeFilter *>(non_negated_filter.second.get());
    return std::make_pair(
        non_negated_filter.first,
        std::make_shared<NegatedBigIntRangeFilter>(range_filter->getLower(), range_filter->getUpper(), range_filter->testNull()));
}

template <is_float T>
bool FloatRangeFilterFactory<T>::validate(const ActionsDAG::Node & node)
{
    if (!isCompareColumnWithConst(node))
        return false;
    const auto * input_node = getInputNode(node);
    auto input_type = removeNullable(input_node->result_type);
    if (!isFloat(input_type))
        return false;
    static const std::unordered_set<String> supported_functions = {"less", "greater", "lessOrEquals", "greaterOrEquals"};
    if (!supported_functions.contains(node.function_base->getName()))
        return false;
    return true;
}

template <is_float T>
NamedColumnFilter FloatRangeFilterFactory<T>::create(const ActionsDAG::Node & node)
{
    const auto * input_node = getInputNode(node);
    auto name = input_node->result_name;
    auto constant_nodes = getConstantNode(node);
    auto func_name = node.function_base->getName();
    T value;
    if constexpr (std::is_same_v<T, Float32>)
        value = constant_nodes.front()->column->getFloat32(0);
    else
        value = constant_nodes.front()->column->getFloat64(0);
    ColumnFilterPtr filter = nullptr;
    if (func_name == "less")
    {
        filter = std::make_shared<FloatRangeFilter<T>>(-std::numeric_limits<T>::infinity(), true, false, value, false, false, false);
    }
    else if (func_name == "greater")
    {
        filter = std::make_shared<FloatRangeFilter<T>>(value, false, false, std::numeric_limits<T>::infinity(), true, false, false);
    }
    else if (func_name == "lessOrEquals")
    {
        filter = std::make_shared<FloatRangeFilter<T>>(-std::numeric_limits<T>::infinity(), true, true, value, false, false, false);
    }
    else if (func_name == "greaterOrEquals")
    {
        filter = std::make_shared<FloatRangeFilter<T>>(value, false, false, std::numeric_limits<T>::infinity(), true, true, false);
    }
    if (filter)
        return std::make_pair(name, filter);
    throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported filter function {}", func_name);
}

template class FloatRangeFilterFactory<Float32>;
template class FloatRangeFilterFactory<Float64>;

bool BytesValuesFilterFactory::validate(const ActionsDAG::Node & node)
{
    if (!isFunctionNodeWithInput(node))
        return false;
    const auto * input_node = getInputNode(node);
    auto input_type = removeNullable(input_node->result_type);
    if (!isString(input_type))
        return false;
    static const std::unordered_set<String> supported_functions = {"equals", "in"};
    if (!supported_functions.contains(node.function_base->getName()))
        return false;
    return true;
}

Strings extractStringsForInClause(const ActionsDAG::Node & node)
{
    auto constant_nodes = getConstantNode(node);
    const IColumn * column_set_ptr;
    if (const auto * arg = checkAndGetColumn<const ColumnConst>(constant_nodes.front()->column.get()))
        column_set_ptr = &arg->getDataColumn();
    else
        column_set_ptr = constant_nodes.front()->column.get();
    const auto * column_set = checkAndGetColumn<const ColumnSet>(column_set_ptr);
    if (!column_set)
        throw DB::Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Only ColumnSet is supported in IN or notIn clause, but got {}", column_set_ptr->getName());
    auto set = column_set->getData()->get();
    if (set->getSetElements().size() != 1)
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "Only one set element is supported in IN clause");
    auto elements = set->getSetElements().front();
    std::vector<String> values;
    values.reserve(elements->size());
    for (size_t i = 0; i < elements->size(); ++i)
    {
        auto value = elements->getDataAt(i);
        String str;
        str.resize(value.size);
        memcpy(str.data(), value.data, value.size);
        values.emplace_back(str);
    }
    return values;
}

NamedColumnFilter BytesValuesFilterFactory::create(const ActionsDAG::Node & node)
{
    const auto * input_node = getInputNode(node);
    auto name = input_node->result_name;
    auto constant_nodes = getConstantNode(node);
    auto func_name = node.function_base->getName();
    ColumnFilterPtr filter = nullptr;
    if (func_name == "equals")
    {
        auto value = constant_nodes.front()->column->getDataAt(0);
        String str;
        str.resize(value.size);
        memcpy(str.data(), value.data, value.size);
        std::vector<String> values = {str};
        filter = std::make_shared<BytesValuesFilter>(values, false);
    }
    else if (func_name == "in")
    {
        auto values = extractStringsForInClause(node);
        filter = std::make_shared<BytesValuesFilter>(values, false);
    }
    if (filter)
    {
        return std::make_pair(name, filter);
    }
    throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported filter function {}", func_name);
}
bool NegatedBytesValuesFilterFactory::validate(const ActionsDAG::Node & node)
{
    if (isFunctionNodeWithInput(node) && (node.function_base->getName() == "notEquals" || node.function_base->getName() == "notIn")
        && isStringInput(node))
        return true;
    if (!isNotFunctionNode(node))
        return false;
    const auto * child = getFirstChildNode(node);
    return non_negated_factory.validate(*child);
}
NamedColumnFilter NegatedBytesValuesFilterFactory::create(const ActionsDAG::Node & node)
{

    auto func_name = node.function_base->getName();
    auto constant_nodes = getConstantNode(node);
    ColumnFilterPtr filter;
    if (func_name == "notEquals")
    {
        const auto * input_node = getInputNode(node);
        auto value = constant_nodes.front()->column->getDataAt(0);
        String str;
        str.resize(value.size);
        memcpy(str.data(), value.data, value.size);
        std::vector<String> values = {str};
        filter = std::make_shared<NegatedBytesValuesFilter>(values, false);
        return std::make_pair(input_node->result_name, filter);
    }
    else if (func_name == "notIn")
    {
        const auto * input_node = getInputNode(node);
        auto values = extractStringsForInClause(node);
        filter = std::make_shared<NegatedBytesValuesFilter>(values, false);
        return std::make_pair(input_node->result_name, filter);
    }
    const auto * child = getFirstChildNode(node);
    auto non_negated_filter = non_negated_factory.create(*child);
    BytesValuesFilter * bytes_values_filter = dynamic_cast<BytesValuesFilter *>(non_negated_filter.second.get());
    Strings values;
    values.insert(values.end(), bytes_values_filter->getValues().begin(), bytes_values_filter->getValues().end());
    return std::make_pair(non_negated_filter.first, std::make_shared<NegatedBytesValuesFilter>(values, bytes_values_filter->testNull()));
}

bool IsNullFilterFactory::validate(const ActionsDAG::Node & node)
{
    if (!isFunctionNodeWithInput(node) || node.function_base->getName() != "isNull")
        return false;
    if (!isIntInput(node) && !isStringInput(node))
        return false;
    return true;
}

NamedColumnFilter IsNullFilterFactory::create(const ActionsDAG::Node & node)
{
    const auto * input_node = getInputNode(node);
    auto name = input_node->result_name;
    auto filter = std::make_shared<IsNullFilter>();
    return std::make_pair(name, filter);
}

bool IsNotNullFilterFactory::validate(const ActionsDAG::Node & node)
{
    if (isFunctionNodeWithInput(node) && node.function_base->getName() == "isNotNull"
        && (isIntInput(node) || isStringInput(node)))
        return true;
    if (!isNotFunctionNode(node))
        return false;
    const auto * child = getFirstChildNode(node);
    return non_negated_factory.validate(*child);
}

NamedColumnFilter IsNotNullFilterFactory::create(const ActionsDAG::Node & node)
{
    auto func_name = node.function_base->getName();
    auto constant_nodes = getConstantNode(node);
    ColumnFilterPtr filter;
    if (func_name == "isNotNull")
    {
        const auto * input_node = getInputNode(node);
        auto name = input_node->result_name;
        filter = std::make_shared<IsNotNullFilter>();
        return std::make_pair(name, filter);
    }
    const auto * child = getFirstChildNode(node);
    auto non_negated_filter = non_negated_factory.create(*child);
    return std::make_pair(non_negated_filter.first, std::make_shared<IsNotNullFilter>());
}

std::vector<ColumnFilterFactoryPtr> ColumnFilterFactory::allFactories()
{
    static const std::vector<ColumnFilterFactoryPtr> factories = {
        std::make_shared<BigIntRangeFilterFactory>(),
        std::make_shared<NegatedBigIntRangeFilterFactory>(),
        std::make_shared<BytesValuesFilterFactory>(),
        std::make_shared<NegatedBytesValuesFilterFactory>(),
        std::make_shared<FloatRangeFilterFactory<Float32>>(),
        std::make_shared<FloatRangeFilterFactory<Float64>>(),
        std::make_shared<IsNullFilterFactory>(),
        std::make_shared<IsNotNullFilterFactory>()
    };
    return factories;
}
}
