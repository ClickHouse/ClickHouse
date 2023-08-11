#include <optional>
#include <numeric>

#include <Storages/Statistic/Statistic.h>
#include <Storages/StatisticsDescription.h>
#include <Common/Exception.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
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
    ColumnEstimator estimator;
    if (it != column_estimators.end())
    {
        estimator = it->second;
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
        return estimator.estimateLess(val) / total_count;
    }
    else if (op == "greater" || op == "greaterThan")
    {
        return estimator.estimateLess(val) / total_count;
    }
    else
        return default_unknown_cond_factor;
}

StatisticPtr TDigestCreator(const StatisticDescription & stat)
{
    if (stat.column_names.size() != 1)
    {
        /// throw
    }

    /// TODO: check column data types.
    return StatisticPtr(new TDigestStatistic(stat));
}

void MergeTreeStatisticFactory::registerCreator(const std::string & stat_type, Creator creator)
{
    if (!creators.emplace(stat_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticFactory: the statistic creator type {} is not unique", stat_type);
}

MergeTreeStatisticFactory::MergeTreeStatisticFactory()
{
    registerCreator("t_digest", TDigestCreator);

    ///registerCreator("cm_sketch", CMSketchCreator);
}

MergeTreeStatisticFactory & MergeTreeStatisticFactory::instance()
{
    static MergeTreeStatisticFactory instance;
    return instance;
}

StatisticPtr MergeTreeStatisticFactory::get(const StatisticDescription & stat) const
{
    auto it = creators.find(stat.type);
    if (it == creators.end())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Unknown Statistic type '{}'. Available types: {}", stat.type,
                std::accumulate(creators.cbegin(), creators.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            else
                                return left + ", " + right.first;
                        })
                );
    }
    return std::make_shared<TDigestStatistic>(stat);
}

Statistics MergeTreeStatisticFactory::getMany(const std::vector<StatisticDescription> & stats) const
{
    Statistics result;
    for (const auto & stat : stats)
        result.push_back(get(stat));
    return result;
}

}
