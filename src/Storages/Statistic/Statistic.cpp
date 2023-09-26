#include <optional>
#include <numeric>

#include <DataTypes/DataTypeNullable.h>
#include <Storages/Statistic/Statistic.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int ILLEGAL_STATISTIC;
}


std::optional<std::string> ConditionEstimator::extractSingleColumn(const RPNBuilderTreeNode & node) const
{
    if (node.isConstant())
    {
        return std::nullopt;
    }

    if (!node.isFunction())
    {
        auto column_name = node.getColumnName();
        return {column_name};
    }

    auto function_node = node.toFunctionNode();
    size_t arguments_size = function_node.getArgumentsSize();
    std::optional<std::string> result;
    for (size_t i = 0; i < arguments_size; ++i)
    {
        auto function_argument = function_node.getArgumentAt(i);
        auto subresult = extractSingleColumn(function_argument);
        if (subresult == std::nullopt)
            continue;
        else if (subresult == "")
            return "";
        else if (result == std::nullopt)
            result = subresult;
        else if (result.value() != subresult.value())
            return "";
    }
    return result;
}

std::pair<std::string, Float64> ConditionEstimator::extractBinaryOp(const RPNBuilderTreeNode & node, const std::string & column_name) const
{
    if (!node.isFunction())
        return {};

    auto function_node = node.toFunctionNode();
    if (function_node.getArgumentsSize() != 2)
        return {};

    std::string function_name = function_node.getFunctionName();

    auto lhs_argument = function_node.getArgumentAt(0);
    auto rhs_argument = function_node.getArgumentAt(1);

    auto lhs_argument_column_name = lhs_argument.getColumnName();
    auto rhs_argument_column_name = rhs_argument.getColumnName();

    bool lhs_argument_is_column = column_name == (lhs_argument_column_name);
    bool rhs_argument_is_column = column_name == (rhs_argument_column_name);

    bool lhs_argument_is_constant = lhs_argument.isConstant();
    bool rhs_argument_is_constant = rhs_argument.isConstant();

    RPNBuilderTreeNode * constant_node = nullptr;

    if (lhs_argument_is_column && rhs_argument_is_constant)
        constant_node = &rhs_argument;
    else if (lhs_argument_is_constant && rhs_argument_is_column)
        constant_node = &lhs_argument;
    else
        return {};

    Field output_value;
    DataTypePtr output_type;
    if (!constant_node->tryGetConstant(output_value, output_type))
        return {};

    const auto type = output_value.getType();
    Float64 value;
    if (type == Field::Types::Int64)
        value = output_value.get<Int64>();
    else if (type == Field::Types::UInt64)
        value = output_value.get<UInt64>();
    else if (type == Field::Types::Float64)
        value = output_value.get<Float64>();
    else
        return {};
    return std::make_pair(function_name, value);
}

Float64 ConditionEstimator::estimateSelectivity(const RPNBuilderTreeNode & node) const
{
    auto col = extractSingleColumn(node);
    if (col == std::nullopt || col == "")
    {
        return default_unknown_cond_factor;
    }
    auto it = column_estimators.find(col.value());

    /// If there the estimator of the column is not found or there are no data at all,
    /// we use dummy estimation.
    bool dummy = total_count == 0;
    ColumnEstimator estimator;
    if (it != column_estimators.end())
    {
        estimator = it->second;
    }
    else
    {
        dummy = true;
    }
    auto [op, val] = extractBinaryOp(node, col.value());
    if (op == "equals")
    {
        if (val < - threshold || val > threshold)
            return default_normal_cond_factor;
        else
            return default_good_cond_factor;
    }
    else if (op == "less" || op == "lessThan")
    {
        if (dummy)
            return default_normal_cond_factor;
        return estimator.estimateLess(val) / total_count;
    }
    else if (op == "greater" || op == "greaterThan")
    {
        if (dummy)
            return default_normal_cond_factor;
        return estimator.estimateGreater(val) / total_count;
    }
    else
        return default_unknown_cond_factor;
}

StatisticPtr TDigestCreator(const StatisticDescription & stat)
{
    /// TODO: check column data types.
    return StatisticPtr(new TDigestStatistic(stat));
}

void TDigestValidator(const StatisticDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTIC, "TDigest does not support type {}", data_type->getName());
}

void MergeTreeStatisticFactory::registerCreator(StatisticType stat_type, Creator creator)
{
    if (!creators.emplace(stat_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticFactory: the statistic creator type {} is not unique", stat_type);
}

void MergeTreeStatisticFactory::registerValidator(StatisticType stat_type, Validator validator)
{
    if (!validators.emplace(stat_type, std::move(validator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticFactory: the statistic validator type {} is not unique", stat_type);

}

MergeTreeStatisticFactory::MergeTreeStatisticFactory()
{
    registerCreator(TDigest, TDigestCreator);
    registerValidator(TDigest, TDigestValidator);
}

MergeTreeStatisticFactory & MergeTreeStatisticFactory::instance()
{
    static MergeTreeStatisticFactory instance;
    return instance;
}

void MergeTreeStatisticFactory::validate(const StatisticDescription & stat, DataTypePtr data_type) const
{
    auto it = validators.find(stat.type);
    if (it == validators.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown Statistic type '{}'", stat.type);
    }
    it->second(stat, data_type);
}

StatisticPtr MergeTreeStatisticFactory::get(const StatisticDescription & stat) const
{
    auto it = creators.find(stat.type);
    if (it == creators.end())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Unknown Statistic type '{}'. Available types: tdigest", stat.type);
    }
    return std::make_shared<TDigestStatistic>(stat);
}

Statistics MergeTreeStatisticFactory::getMany(const ColumnsDescription & columns) const
{
    Statistics result;
    for (const auto & col : columns)
        if (col.stat)
            result.push_back(get(*col.stat));
    return result;
}

}
