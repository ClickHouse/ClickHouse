#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <stack>
#include <iostream>

#include <Common/logger_useful.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

RelationProfile ConditionSelectivityEstimator::estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * filter, const ActionsDAG::Node * prewhere) const
{
    if (filter == nullptr && prewhere == nullptr)
    {
        return estimateRelationProfile();
    }
    else if (filter == nullptr)
    {
        return estimateRelationProfile(metadata, prewhere);
    }
    else if (prewhere == nullptr)
    {
        return estimateRelationProfile(metadata, filter);
    }
    std::vector<RPNElement> rpn = RPNBuilder<RPNElement>(filter, getContext(), [&](const RPNBuilderTreeNode & node_, RPNElement & out)
    {
        return extractAtomFromTree(metadata, node_, out);
    }).extractRPN();
    std::vector<RPNElement> prewhere_rpn = RPNBuilder<RPNElement>(prewhere, getContext(), [&](const RPNBuilderTreeNode & node_, RPNElement & out)
    {
        return extractAtomFromTree(metadata, node_, out);
    }).extractRPN();
    rpn.insert(rpn.end(), prewhere_rpn.begin(), prewhere_rpn.end());
    RPNElement last_rpn;
    last_rpn.function = RPNElement::FUNCTION_AND;
    rpn.push_back(last_rpn);
    return estimateRelationProfileImpl(rpn);
}

RelationProfile ConditionSelectivityEstimator::estimateRelationProfile(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node) const
{
    std::vector<RPNElement> rpn = RPNBuilder<RPNElement>(node, [&](const RPNBuilderTreeNode & node_, RPNElement & out)
    {
        return extractAtomFromTree(metadata, node_, out);
    }).extractRPN();
    return estimateRelationProfileImpl(rpn);
}

RelationProfile ConditionSelectivityEstimator::estimateRelationProfileImpl(std::vector<RPNElement> & rpn) const
{
    /// walk through the tree and calculate selectivity for every rpn node.
    std::stack<RPNElement *> rpn_stack;
    for (auto & element : rpn)
    {
        switch (element.function)
        {
            /// for a AND b / a OR b, we check:
            /// 1. if a / b is always true or false
            /// 2. if a / b is AND / OR clause
            /// 2.a if a AND b and a/b is OR clause containing different columns, we don't merge the ranges
            /// 2.b if a OR b and a/b is AND clause containing different columns, we don't merge the ranges
            /// 2.c in other cases, we intersect or union the ranges
            /// 3. if we cannot merge the expressions, we mark the expression as 'finalized' and materialize the selectivity.
            /// 4. we don't merge ranges for finalized expression.
            case RPNElement::FUNCTION_AND:
            case RPNElement::FUNCTION_OR:
            {
                auto* right_element = rpn_stack.top();
                rpn_stack.pop();
                auto* left_element = rpn_stack.top();
                rpn_stack.pop();
                if (right_element->function == RPNElement::ALWAYS_TRUE || left_element->function == RPNElement::ALWAYS_FALSE)
                    rpn_stack.push(element.function == RPNElement::FUNCTION_AND ? left_element : right_element);
                else if (right_element->function == RPNElement::ALWAYS_FALSE || left_element->function == RPNElement::ALWAYS_TRUE)
                    rpn_stack.push(element.function == RPNElement::FUNCTION_AND ? right_element : left_element);
                else if (element.tryToMergeClauses(*left_element, *right_element))
                    rpn_stack.push(&element);
                else
                {
                    left_element->finalize(column_estimators);
                    right_element->finalize(column_estimators);
                    /// P(c1 and c2) = P(c1) * P(c2)
                    if (element.function == RPNElement::FUNCTION_AND)
                        element.selectivity = left_element->selectivity * right_element->selectivity;
                    /// P(c1 or c2) = 1 - (1 - P(c1)) * (1 - P(c2))
                    else
                        element.selectivity = 1-(1-left_element->selectivity)*(1-right_element->selectivity);
                    element.finalized = true;
                    rpn_stack.push(&element);
                }
                break;
            }
            case RPNElement::FUNCTION_NOT:
            {
                auto* last_element = rpn_stack.top();
                if (last_element->finalized && last_element->function != RPNElement::FUNCTION_UNKNOWN)
                    last_element->selectivity = 1 - last_element->selectivity;
                else
                {
                    std::swap(last_element->column_ranges, last_element->column_not_ranges);
                    if (last_element->function == RPNElement::FUNCTION_AND)
                        last_element->function = RPNElement::FUNCTION_OR;
                    else if (last_element->function == RPNElement::FUNCTION_OR)
                        last_element->function = RPNElement::FUNCTION_AND;
                    else if (last_element->function == RPNElement::ALWAYS_FALSE)
                        last_element->function = RPNElement::ALWAYS_TRUE;
                    else if (last_element->function == RPNElement::ALWAYS_TRUE)
                        last_element->function = RPNElement::ALWAYS_FALSE;
                }
                break;
            }
            default:
                rpn_stack.push(&element);
        }
    }
    auto* final_element = rpn_stack.top();
    final_element->finalize(column_estimators);
    RelationProfile result;
    result.rows = static_cast<UInt64>(final_element->selectivity * total_rows);
    for (const auto & [column_name, estimator] : column_estimators)
    {
        UInt64 cardinality = std::min(result.rows, estimator.estimateCardinality());
        result.column_stats.emplace(column_name, cardinality);
    }
    return result;
}

RelationProfile ConditionSelectivityEstimator::estimateRelationProfile() const
{
    RelationProfile result;
    result.rows = total_rows;
    for (const auto & [column_name, estimator] : column_estimators)
    {
        result.column_stats.emplace(column_name, estimator.estimateCardinality());
    }
    return result;
}

RelationProfile ConditionSelectivityEstimator::estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * node) const
{
    RPNBuilderTreeContext tree_context(getContext());
    return estimateRelationProfile(metadata, RPNBuilderTreeNode(node, tree_context));
}

bool ConditionSelectivityEstimator::isStale(const std::vector<DataPartPtr> & data_parts) const
{
    if (data_parts.size() != parts_names.size())
        return true;
    size_t idx = 0;
    for (const auto & data_part : data_parts)
    {
        if (parts_names[idx++] != data_part->name)
            return true;
    }
    return false;
}

bool ConditionSelectivityEstimator::extractAtomFromTree(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node, RPNElement & out) const
{
    const auto * node_dag = node.getDAGNode();
    if (node_dag && node_dag->result_type->equals(DataTypeNullable(std::make_shared<DataTypeNothing>())))
    {
        /// If the inferred result type is Nullable(Nothing) at the query analysis stage,
        /// we don't analyze this node further as its condition will always be false.
        out.function = RPNElement::ALWAYS_FALSE;
        return true;
    }
    Field const_value;
    DataTypePtr const_type;
    String column_name;
    DataTypePtr column_type;

    if (node.isFunction())
    {
        auto func = node.toFunctionNode();
        size_t num_args = func.getArgumentsSize();

        String func_name = func.getFunctionName();
        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        if (num_args == 2)
        {
            const bool is_in_operator = functionIsInOperator(func_name);

            /// If the second argument is built from `ASTNode`, it should fall into next branch, which directly
            /// extracts constant value from `ASTLiteral`. Otherwise we try to build `Set` from `ActionsDAG::Node`,
            /// and extract constant value from it.
            if (is_in_operator && !func.getArgumentAt(1).getASTNode())
            {
                const auto & rhs = func.getArgumentAt(1);
                if (!rhs.isConstant())
                    return false;

                auto future_set = rhs.tryGetPreparedSet();
                if (!future_set)
                    return false;

                auto prepared_set = future_set->buildOrderedSetInplace(rhs.getTreeContext().getQueryContext());
                if (!prepared_set || !prepared_set->hasExplicitSetElements())
                    return false;

                Columns columns = prepared_set->getSetElements();
                if (columns.size() != 1)
                    return false;

                Tuple tuple(columns[0]->size());
                for (size_t i = 0; i < columns[0]->size(); ++i)
                    tuple[i] = (*columns[0])[i];

                const_value = std::move(tuple);
                column_name = func.getArgumentAt(0).getColumnName();
            }
            else if (func.getArgumentAt(1).tryGetConstant(const_value, const_type))
            {
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }
                column_name = func.getArgumentAt(0).getColumnName();
            }
            else if (func.getArgumentAt(0).tryGetConstant(const_value, const_type))
            {
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }

                column_name = func.getArgumentAt(1).getColumnName();
                if (func_name == "less")
                    func_name = "greater";
                else if (func_name == "greater")
                    func_name = "less";
                else if (func_name == "greaterOrEquals")
                    func_name = "lessOrEquals";
                else if (func_name == "lessOrEquals")
                    func_name = "greaterOrEquals";
            }
            else
                return false;

            if (metadata)
            {
                const ColumnDescription * column_desc = metadata->getColumns().tryGet(column_name);
                if (column_desc)
                    column_type = removeLowCardinalityAndNullable(column_desc->type);
            }
            /// In some cases we need to cast the type of const
            bool cast_not_needed = !column_type ||
                ((isNativeInteger(column_type) || isDateTime(column_type))
                && (isNativeInteger(const_type) || isDateTime(const_type)));

            if (!cast_not_needed && !column_type->equals(*const_type))
            {
                if (const_value.getType() == Field::Types::String)
                {
                    const_value = convertFieldToType(const_value, *column_type);
                    if (const_value.isNull())
                        return false;
                }
                else
                {
                    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{column_type, const_type});
                    if (!common_type)
                        return false;

                    if (!const_type->equals(*common_type))
                    {
                        // Replace direct call that throws exception with try version
                        Field converted = tryConvertFieldToType(const_value, *common_type, const_type.get(), {});
                        if (converted.isNull())
                            return false;

                        const_value = converted;
                    }
                    if (!column_type->equals(*common_type))
                    {
                        /// we assume that is "cast(column) < const", will not estimate this condition.
                        return false;
                    }
                }
            }
            const auto atom_it = atom_map.find(func_name);
            atom_it->second(out, column_name, const_value);
            return true;
        }
    }
    return false;
}

ConditionSelectivityEstimatorBuilder::ConditionSelectivityEstimatorBuilder(ContextPtr context_)
    : estimator(std::make_shared<ConditionSelectivityEstimator>(context_))
{
}

void ConditionSelectivityEstimatorBuilder::incrementRowCount(UInt64 rows)
{
    estimator->total_rows += rows;
}

void ConditionSelectivityEstimatorBuilder::markDataPart(const DataPartPtr & data_part)
{
    estimator->parts_names.push_back(data_part->name);
    estimator->total_rows += data_part->rows_count;
}

void ConditionSelectivityEstimatorBuilder::addStatistics(ColumnStatisticsPtr column_stats)
{
    if (column_stats != nullptr)
    {
        has_data = true;
        auto & column_estimator = estimator->column_estimators[column_stats->getColumnName()];
        if (column_estimator.stats == nullptr)
            column_estimator.stats = column_stats;
        else
            column_estimator.stats->merge(column_stats);
    }
}

ConditionSelectivityEstimatorPtr ConditionSelectivityEstimatorBuilder::getEstimator() const
{
    if (!has_data)
        return nullptr;
    return estimator;
}

Float64 ConditionSelectivityEstimator::ColumnEstimator::estimateRanges(const PlainRanges & ranges) const
{
    Float64 result = 0;
    for (const Range & range : ranges.ranges)
    {
        result += stats->estimateRange(range);
    }
    /// In case that there is an empty statistics.
    if (stats->rowCount() == 0)
        return 0;
    return result / stats->rowCount();
}

UInt64 ConditionSelectivityEstimator::ColumnEstimator::estimateCardinality() const
{
    return stats->estimateCardinality();
}

const ConditionSelectivityEstimator::AtomMap ConditionSelectivityEstimator::atom_map
{
        {
            "notEquals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_not_ranges.emplace(column, Range(value));
            }
        },
        {
            "equals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range(value));
            }
        },
        {
            "in",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                Ranges ranges;
                for (const Field & field : value.safeGet<Tuple>())
                {
                    ranges.emplace_back(field);
                }
                out.column_ranges.emplace(column, PlainRanges(ranges, /*intersect*/ true, /*ordered*/ false));
            }
        },
        {
            "notIn",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                Ranges ranges;
                for (const Field & field : value.safeGet<Tuple>())
                {
                    ranges.emplace_back(field);
                }
                out.column_not_ranges.emplace(column, PlainRanges(ranges, /*intersect*/ true, /*ordered*/ false));
            }
        },
        {
            "less",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createRightBounded(value, false));
            }
        },
        {
            "greater",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createLeftBounded(value, false));
            }
        },
        {
            "lessOrEquals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createRightBounded(value, true));
            }
        },
        {
            "greaterOrEquals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createLeftBounded(value, true));
            }
        }
};

/// merge CNF or DNF
bool ConditionSelectivityEstimator::RPNElement::tryToMergeClauses(RPNElement & lhs, RPNElement & rhs)
{
    auto canMergeWith = [](const RPNElement & e, Function function_to_merge)
    {
        return (e.function == FUNCTION_IN_RANGE
                /// if the sub-clause is also cnf/dnf, it's good to merge
                || e.function == function_to_merge
                /// if the sub-clause is different, but has only one column, it also works, e.g
                /// (a > 0 and a < 5) or (a > 3 and a < 10) can be merged to (a > 0 and a < 10)
                || (e.column_ranges.size() + e.column_not_ranges.size()) == 1
                || e.function == FUNCTION_UNKNOWN)
                && !e.finalized;
    };
    /// we will merge normal expression and not expression separately.
    auto merge_column_ranges = [this](ColumnRanges & result_ranges, ColumnRanges & l_ranges, ColumnRanges & r_ranges, bool is_not)
    {
        for (auto & [column_name, ranges] : l_ranges)
        {
            auto rit = r_ranges.find(column_name);
            if (rit != r_ranges.end())
            {
                /// not a or not b means not (a and b), so we should use intersect here.
                if ((function == FUNCTION_AND && !is_not) || (function == FUNCTION_OR && is_not))
                    result_ranges.emplace(column_name, ranges.intersectWith(rit->second));
                else
                    result_ranges.emplace(column_name, ranges.unionWith(rit->second));
            }
            else
                result_ranges.emplace(column_name, ranges);
        }
        for (auto & [column_name, ranges] : r_ranges)
        {
            if (!l_ranges.contains(column_name))
                result_ranges.emplace(column_name, ranges);
        }
    };
    if (canMergeWith(lhs, function) && canMergeWith(rhs, function))
    {
        merge_column_ranges(column_ranges, lhs.column_ranges, rhs.column_ranges, false);
        merge_column_ranges(column_not_ranges, lhs.column_not_ranges, rhs.column_not_ranges, true);
        return true;
    }
    return false;
}

/// finalization of a expression means we would calculate the seletivity and no longer analyze ranges further.
void ConditionSelectivityEstimator::RPNElement::finalize(const ColumnEstimators & column_estimators_)
{
    if (finalized)
        return;

    finalized = true;

    if (function == FUNCTION_UNKNOWN)
    {
        selectivity = default_unknown_cond_factor;
        return;
    }

    auto estimate_unknown_ranges = [&](const PlainRanges & ranges)
    {
        Float64 equal_selectivity = 0;
        for (const Range & range : ranges.ranges)
        {
            if (range.isInfinite())
                return 1.0;

            if (range.left == range.right)
                equal_selectivity += default_cond_equal_factor;
            else
                return default_cond_range_factor;
        }
        return std::min(equal_selectivity, 1.0);
    };

    std::vector<Float64> estimate_results;
    for (const auto & [column_name, ranges] : column_ranges)
    {
        auto it = column_estimators_.find(column_name);
        if (it == column_estimators_.end())
        {
            estimate_results.emplace_back(estimate_unknown_ranges(ranges));
        }
        else
            estimate_results.emplace_back(it->second.estimateRanges(ranges));
    }

    for (const auto & [column_name, ranges] : column_not_ranges)
    {
        auto it = column_estimators_.find(column_name);
        if (it == column_estimators_.end())
        {
            estimate_results.emplace_back(1 - estimate_unknown_ranges(ranges));
        }
        else
            estimate_results.emplace_back(1.0 - it->second.estimateRanges(ranges));
    }

    selectivity = 1.0;
    for (const auto & estimate_result : estimate_results)
    {
        if (function == FUNCTION_OR)
            selectivity *= 1 - estimate_result;
        else
            selectivity *= estimate_result;
    }
    if (function == FUNCTION_OR)
        selectivity = 1 - selectivity;
}
}
