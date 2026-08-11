#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <stack>
#include <cmath>
#include <limits>
#include <set>

#include <Common/logger_useful.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Utils.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Formats/ParseError.h>


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
    return estimateRelationProfileImpl(rpn, metadata);
}

RelationProfile ConditionSelectivityEstimator::estimateRelationProfile(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node) const
{
    std::vector<RPNElement> rpn = RPNBuilder<RPNElement>(node, [&](const RPNBuilderTreeNode & node_, RPNElement & out)
    {
        return extractAtomFromTree(metadata, node_, out);
    }).extractRPN();
    return estimateRelationProfileImpl(rpn, metadata);
}

RelationProfile ConditionSelectivityEstimator::estimateRelationProfile(
    const StorageMetadataPtr & metadata,
    const std::vector<RPNBuilderTreeNode> & nodes) const
{
    if (nodes.empty())
        return estimateRelationProfile();

    /// Build a combined RPN sequence by concatenating per-node RPNs and inserting an
    /// FUNCTION_AND token after every node past the first (standard postfix AND for
    /// left-associative evaluation):
    ///   1 node  → rpn_0
    ///   2 nodes → rpn_0 | rpn_1 | AND
    ///   3 nodes → rpn_0 | rpn_1 | AND | rpn_2 | AND  = (r0 ∧ r1) ∧ r2
    std::vector<RPNElement> combined_rpn;
    for (size_t i = 0; i < nodes.size(); ++i)
    {
        auto rpn = RPNBuilder<RPNElement>(nodes[i], [&](const RPNBuilderTreeNode & node_, RPNElement & out)
        {
            return extractAtomFromTree(metadata, node_, out);
        }).extractRPN();
        combined_rpn.insert(combined_rpn.end(), rpn.begin(), rpn.end());
        if (i > 0)
        {
            RPNElement and_elem;
            and_elem.function = RPNElement::FUNCTION_AND;
            combined_rpn.push_back(and_elem);
        }
    }
    return estimateRelationProfileImpl(combined_rpn, metadata);
}

static bool isCompatibleStatistics(const StorageMetadataPtr & metadata, const ColumnStatisticsPtr & stats, const String & column_name)
{
    if (!metadata)
        return true;

    const auto * column = metadata->getColumns().tryGet(column_name);
    if (!column)
        return false;

    /// Skip if the column statistics has outdated data type.
    /// It can happen after ALTER MODIFY COLUMN until mutations is not materialized in the data part.
    return column->type->equals(*stats->getDataType());
}

namespace
{

struct ColumnNullShares
{
    Float64 null = 0.0;
    Float64 non_null = 1.0;
};

bool isSupportedColumnComparisonType(const DataTypePtr & type)
{
    return isNativeNumber(type) || isDate(type) || isDate32(type) || isDateTime(type) || isDateTime64(type);
}

Float64 clampProbability(Float64 value)
{
    if (!std::isfinite(value))
        return 0.0;
    return std::max(0.0, std::min(1.0, value));
}

bool tryConvertFieldLosslesslyToCommonType(
    const Field & value,
    const DataTypePtr & from_type,
    const DataTypePtr & common_type,
    Field & out)
{
    if (value.isNull() || value.isNaN())
        return false;

    if (from_type->equals(*common_type))
    {
        out = value;
        return true;
    }

    Field converted = tryConvertFieldToType(value, *common_type, from_type.get(), {}, /*strict=*/true);
    if (converted.isNull() || converted.isNaN())
        return false;

    /// Min/max proofs must be conservative: if converting a boundary to the common
    /// comparison type loses information, the disjoint proof may become wrong.
    Field round_trip = tryConvertFieldToType(converted, *from_type, common_type.get(), {}, /*strict=*/true);
    if (round_trip.isNull() || round_trip != value)
        return false;

    out = converted;
    return true;
}

bool isComparisonFunction(const String & function_name)
{
    return function_name == "equals" || function_name == "notEquals"
        || function_name == "less" || function_name == "lessOrEquals"
        || function_name == "greater" || function_name == "greaterOrEquals";
}

DataTypePtr getFieldTypeForExactConversion(const Field & value, const DataTypePtr & fallback_type)
{
    if (value.isNull())
        return fallback_type;

    /// AST-only tests may provide a dummy constant block type for every literal.
    /// The Field's own type is a better hint for exact numeric rewrites.
    return applyVisitor(FieldToDataType(), value);
}

bool tryConvertFieldToInt64Exactly(const Field & value, const DataTypePtr & from_type, Int64 & out)
{
    DataTypePtr effective_from_type = getFieldTypeForExactConversion(value, from_type);
    if (!effective_from_type || value.isNull() || value.isNaN())
        return false;

    auto int64_type = std::make_shared<DataTypeInt64>();
    Field converted = tryConvertFieldToType(value, *int64_type, effective_from_type.get(), {}, /*strict=*/true);
    if (converted.isNull() || converted.getType() != Field::Types::Int64)
        return false;

    Field round_trip = tryConvertFieldToType(converted, *effective_from_type, int64_type.get(), {}, /*strict=*/true);
    if (round_trip.isNull() || round_trip != value)
        return false;

    out = converted.safeGet<Int64>();
    return true;
}

bool addInt64Checked(Int64 lhs, Int64 rhs, Int64 & out)
{
    Int128 result = static_cast<Int128>(lhs) + static_cast<Int128>(rhs);
    if (result < std::numeric_limits<Int64>::min() || result > std::numeric_limits<Int64>::max())
        return false;
    out = static_cast<Int64>(result);
    return true;
}

bool subtractInt64Checked(Int64 lhs, Int64 rhs, Int64 & out)
{
    Int128 result = static_cast<Int128>(lhs) - static_cast<Int128>(rhs);
    if (result < std::numeric_limits<Int64>::min() || result > std::numeric_limits<Int64>::max())
        return false;
    out = static_cast<Int64>(result);
    return true;
}

}

RelationProfile ConditionSelectivityEstimator::estimateRelationProfileImpl(std::vector<RPNElement> & rpn, const StorageMetadataPtr & metadata) const
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
                    auto try_combine_null_sensitive_predicate_with_null_check = [&](const RPNElement & predicate, const RPNElement & null_check) -> bool
                    {
                        if (!predicate.finalized || null_check.finalized
                            || !null_check.column_ranges.empty() || !null_check.column_not_ranges.empty()
                            || null_check.null_check_columns.size() + null_check.not_null_check_columns.size() != 1)
                            return false;

                        bool is_null_check = !null_check.null_check_columns.empty();
                        const String & column_name = is_null_check ? *null_check.null_check_columns.begin() : *null_check.not_null_check_columns.begin();
                        if (element.function == RPNElement::FUNCTION_AND)
                        {
                            if ((is_null_check && predicate.not_null_check_columns.contains(column_name))
                                || (!is_null_check && predicate.null_check_columns.contains(column_name)))
                            {
                                element.selectivity = Selectivity();
                                element.finalized = true;
                                return true;
                            }
                        }

                        auto null_share_it = predicate.null_sensitive_column_null_shares.find(column_name);
                        if (null_share_it == predicate.null_sensitive_column_null_shares.end())
                            return false;

                        Float64 null_share = clampProbability(null_share_it->second);
                        Float64 non_null_share = 1.0 - null_share;
                        if (element.function == RPNElement::FUNCTION_AND)
                        {
                            if (is_null_check)
                                element.selectivity = Selectivity(0.0, null_share);
                            else
                                element.selectivity = Selectivity(predicate.selectivity.true_sel, std::max(0.0, predicate.selectivity.null_sel - null_share));
                        }
                        else
                        {
                            if (is_null_check)
                                element.selectivity = Selectivity(
                                    std::min(1.0, predicate.selectivity.true_sel + null_share),
                                    std::max(0.0, predicate.selectivity.null_sel - null_share));
                            else
                                element.selectivity = Selectivity(std::max(predicate.selectivity.true_sel, non_null_share), null_share);
                        }
                        element.selectivity.true_sel = clampProbability(element.selectivity.true_sel);
                        element.selectivity.null_sel = std::max(0.0, std::min(1.0 - element.selectivity.true_sel, element.selectivity.null_sel));
                        element.null_sensitive_column_null_shares = predicate.null_sensitive_column_null_shares;
                        element.null_check_columns = predicate.null_check_columns;
                        element.not_null_check_columns = predicate.not_null_check_columns;
                        if (element.function == RPNElement::FUNCTION_AND)
                        {
                            if (is_null_check)
                                element.null_check_columns.insert(column_name);
                            else
                                element.not_null_check_columns.insert(column_name);
                        }
                        /// `predicate AND col IS NOT NULL` and `predicate OR col IS NULL`
                        /// resolve the predicate's NULL result for `col`; future NULL-sensitive
                        /// combination on the same column must not see the old dependency.
                        if ((element.function == RPNElement::FUNCTION_AND && !is_null_check)
                            || (element.function == RPNElement::FUNCTION_OR && is_null_check))
                            element.null_sensitive_column_null_shares.erase(column_name);
                        element.finalized = true;
                        return true;
                    };

                    auto plain_ranges_contain = [](const PlainRanges & ranges, const Field & value) -> bool
                    {
                        Range point(value);
                        for (const Range & range : ranges.ranges)
                        {
                            if (range.containsRange(point))
                                return true;
                        }
                        return false;
                    };

                    auto try_combine_tuple_predicate_with_filter = [&](const RPNElement & tuple_predicate, const RPNElement & filter) -> bool
                    {
                        if (element.function != RPNElement::FUNCTION_AND || !tuple_predicate.finalized
                            || tuple_predicate.tuple_alternatives.empty() || tuple_predicate.tuple_negated
                            || filter.finalized || filter.function == RPNElement::FUNCTION_OR)
                            return false;

                        std::unordered_set<String> tuple_columns;
                        for (const auto & alternative : tuple_predicate.tuple_alternatives)
                        {
                            for (const auto & equality : alternative.equalities)
                                tuple_columns.insert(equality.first);
                        }

                        bool touches_tuple_column = false;
                        for (const String & column_name : tuple_columns)
                        {
                            if (filter.column_ranges.contains(column_name) || filter.column_not_ranges.contains(column_name)
                                || filter.null_check_columns.contains(column_name) || filter.not_null_check_columns.contains(column_name))
                            {
                                touches_tuple_column = true;
                                break;
                            }
                        }
                        if (!touches_tuple_column)
                            return false;

                        std::vector<RPNElement::TupleAlternative> surviving_alternatives;
                        Float64 tuple_selectivity = 0.0;
                        for (const auto & alternative : tuple_predicate.tuple_alternatives)
                        {
                            bool possible = true;
                            for (const auto & [column_name, value] : alternative.equalities)
                            {
                                if (filter.null_check_columns.contains(column_name))
                                {
                                    possible = false;
                                    break;
                                }

                                if (auto it = filter.column_ranges.find(column_name);
                                    it != filter.column_ranges.end() && !plain_ranges_contain(it->second, value))
                                {
                                    possible = false;
                                    break;
                                }

                                if (auto it = filter.column_not_ranges.find(column_name);
                                    it != filter.column_not_ranges.end() && plain_ranges_contain(it->second, value))
                                {
                                    possible = false;
                                    break;
                                }
                            }

                            if (possible)
                            {
                                surviving_alternatives.push_back(alternative);
                                tuple_selectivity = std::min(1.0, tuple_selectivity + alternative.selectivity);
                            }
                        }

                        if (surviving_alternatives.empty())
                        {
                            element.selectivity = Selectivity();
                            element.finalized = true;
                            return true;
                        }

                        RPNElement residual_filter = filter;
                        for (const String & column_name : tuple_columns)
                        {
                            residual_filter.column_ranges.erase(column_name);
                            residual_filter.column_not_ranges.erase(column_name);
                            residual_filter.null_check_columns.erase(column_name);
                            residual_filter.not_null_check_columns.erase(column_name);
                        }
                        residual_filter.finalize(column_estimators, metadata);

                        element.selectivity = Selectivity(tuple_selectivity, 0.0).applyAnd(residual_filter.selectivity);
                        element.tuple_alternatives = std::move(surviving_alternatives);
                        element.finalized = true;
                        return true;
                    };

                    if (try_combine_null_sensitive_predicate_with_null_check(*left_element, *right_element)
                        || try_combine_null_sensitive_predicate_with_null_check(*right_element, *left_element)
                        || try_combine_tuple_predicate_with_filter(*left_element, *right_element)
                        || try_combine_tuple_predicate_with_filter(*right_element, *left_element))
                    {
                        rpn_stack.push(&element);
                    }
                    else
                    {
                        left_element->finalize(column_estimators, metadata);
                        right_element->finalize(column_estimators, metadata);
                        /// P(c1 and c2) = P(c1) * P(c2)
                        if (element.function == RPNElement::FUNCTION_AND)
                            element.selectivity = left_element->selectivity.applyAnd(right_element->selectivity);
                        /// P(c1 or c2) = 1 - (1 - P(c1)) * (1 - P(c2))
                        else
                            element.selectivity = left_element->selectivity.applyOr(right_element->selectivity);
                        element.finalized = true;
                        rpn_stack.push(&element);
                    }
                }
                break;
            }
            case RPNElement::FUNCTION_NOT:
            {
                auto* last_element = rpn_stack.top();
                if (last_element->finalized)
                    last_element->selectivity = last_element->selectivity.applyNot();
                else
                {
                    std::swap(last_element->column_ranges, last_element->column_not_ranges);
                    std::swap(last_element->null_check_columns, last_element->not_null_check_columns);
                    switch (last_element->function)
                    {
                        case RPNElement::FUNCTION_AND:        last_element->function = RPNElement::FUNCTION_OR;       break;
                        case RPNElement::FUNCTION_OR:         last_element->function = RPNElement::FUNCTION_AND;      break;
                        case RPNElement::FUNCTION_IS_NULL:    last_element->function = RPNElement::FUNCTION_IS_NOT_NULL; break;
                        case RPNElement::FUNCTION_IS_NOT_NULL:last_element->function = RPNElement::FUNCTION_IS_NULL;  break;
                        case RPNElement::ALWAYS_FALSE:        last_element->function = RPNElement::ALWAYS_TRUE;       break;
                        case RPNElement::ALWAYS_TRUE:         last_element->function = RPNElement::ALWAYS_FALSE;      break;
                        default: break;
                    }
                }
                break;
            }
            default:
                rpn_stack.push(&element);
        }
    }
    auto * final_element = rpn_stack.top();
    final_element->finalize(column_estimators, metadata);
    RelationProfile result;
    Float64 final_rows = final_element->selectivity.true_sel * static_cast<Float64>(total_rows);
    /// Clamp to [0, total_rows] and handle NaN/Inf to avoid undefined behavior
    /// in the float-to-UInt64 cast below (UBSAN float-cast-overflow).
    if (!std::isfinite(final_rows) || final_rows < 0)
        final_rows = 0;
    else if (final_rows > static_cast<Float64>(total_rows))
        final_rows = static_cast<Float64>(total_rows);
    result.rows = static_cast<UInt64>(final_rows);
    for (const auto & [column_name, estimator] : column_estimators)
    {
        if (!isCompatibleStatistics(metadata, estimator.stats, column_name))
            continue;

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

bool ConditionSelectivityEstimator::tryExtractColumnComparison(
    const StorageMetadataPtr & metadata,
    const String & function_name,
    const RPNBuilderTreeNode & lhs,
    const RPNBuilderTreeNode & rhs,
    RPNElement & out) const
{
    if (!metadata)
        return false;

    if (function_name != "equals" && function_name != "notEquals"
        && function_name != "less" && function_name != "lessOrEquals"
        && function_name != "greater" && function_name != "greaterOrEquals")
        return false;

    if (lhs.isConstant() || rhs.isConstant())
        return false;

    const String lhs_column_name = lhs.getColumnName();
    const String rhs_column_name = rhs.getColumnName();
    const ColumnDescription * lhs_column = metadata->getColumns().tryGet(lhs_column_name);
    const ColumnDescription * rhs_column = metadata->getColumns().tryGet(rhs_column_name);
    if (!lhs_column || !rhs_column)
        return false;

    auto get_estimator = [&](const String & column_name) -> const ColumnEstimator *
    {
        auto it = column_estimators.find(column_name);
        if (it == column_estimators.end() || !isCompatibleStatistics(metadata, it->second.stats, column_name))
            return nullptr;
        return &it->second;
    };

    auto get_null_shares = [&](const ColumnDescription & column, const ColumnEstimator * estimator) -> ColumnNullShares
    {
        ColumnNullShares result;
        if (estimator && estimator->stats->getNumRows() != 0 && estimator->stats->hasNullCount())
        {
            Float64 rows = static_cast<Float64>(estimator->stats->getNumRows());
            result.null = static_cast<Float64>(estimator->stats->getNullCount()) / rows;
        }
        else if (!isNullableOrLowCardinalityNullable(column.type))
        {
            result.null = 0.0;
        }
        else
        {
            /// Nullable column but no Basic/null-count statistic. Mirror the existing IS NULL
            /// fallback instead of treating getNullCount()==0 as proof of no NULLs.
            result.null = default_cond_equal_factor;
        }

        result.null = clampProbability(result.null);
        result.non_null = 1.0 - result.null;
        return result;
    };

    const ColumnEstimator * lhs_estimator = get_estimator(lhs_column_name);
    const ColumnEstimator * rhs_estimator = get_estimator(rhs_column_name);
    const ColumnNullShares lhs_nulls = get_null_shares(*lhs_column, lhs_estimator);
    const ColumnNullShares rhs_nulls = get_null_shares(*rhs_column, rhs_estimator);

    auto set_finalized_selectivity = [&](Float64 true_sel, Float64 null_sel)
    {
        out.selectivity = Selectivity(clampProbability(true_sel), clampProbability(null_sel));
        out.null_sensitive_column_null_shares[lhs_column_name] = lhs_nulls.null;
        if (rhs_column_name != lhs_column_name)
            out.null_sensitive_column_null_shares[rhs_column_name] = rhs_nulls.null;
        out.finalized = true;
    };

    const DataTypePtr lhs_type = removeLowCardinalityAndNullable(lhs_column->type);
    const DataTypePtr rhs_type = removeLowCardinalityAndNullable(rhs_column->type);

    if (lhs_column_name == rhs_column_name)
    {
        /// Same-column comparisons are deterministic on non-NULL rows, and evaluate to
        /// NULL on NULL rows under regular SQL comparison semantics. Floating-point
        /// comparisons involving NaN are the exception (`NaN = NaN` and `NaN <= NaN`
        /// are false), and statistics do not track NaN counts. Do not claim exact
        /// selectivity for those predicates.
        if (isFloat(lhs_type) && function_name != "less" && function_name != "greater")
            set_finalized_selectivity(default_unknown_cond_factor * lhs_nulls.non_null, lhs_nulls.null);
        else if (function_name == "equals" || function_name == "lessOrEquals" || function_name == "greaterOrEquals")
            set_finalized_selectivity(lhs_nulls.non_null, lhs_nulls.null);
        else
            set_finalized_selectivity(0.0, lhs_nulls.null);
        return true;
    }
    if (!isSupportedColumnComparisonType(lhs_type) || !isSupportedColumnComparisonType(rhs_type))
        return false;

    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{lhs_type, rhs_type});
    if (!common_type || !isSupportedColumnComparisonType(common_type))
        return false;

    const Float64 non_null_both = clampProbability(lhs_nulls.non_null * rhs_nulls.non_null);
    const Float64 null_sel = clampProbability(1.0 - non_null_both);

    auto get_real_ndv = [](const ColumnEstimator * estimator) -> std::optional<UInt64>
    {
        if (!estimator)
            return std::nullopt;
        return estimator->stats->getEstimate().estimated_cardinality;
    };

    auto estimate_equality = [&]() -> Float64
    {
        const auto lhs_ndv = get_real_ndv(lhs_estimator);
        const auto rhs_ndv = get_real_ndv(rhs_estimator);
        if (lhs_ndv && rhs_ndv)
        {
            UInt64 max_ndv = std::max(*lhs_ndv, *rhs_ndv);
            if (max_ndv == 0)
                return 0.0;
            return clampProbability(std::min(non_null_both, non_null_both / static_cast<Float64>(max_ndv)));
        }

        return clampProbability(std::min(non_null_both, default_cond_equal_factor));
    };

    if (function_name == "equals" || function_name == "notEquals")
    {
        const Float64 equality_sel = estimate_equality();
        if (function_name == "equals")
            set_finalized_selectivity(equality_sel, null_sel);
        else
            set_finalized_selectivity(std::max(0.0, non_null_both - equality_sel), null_sel);
        return true;
    }

    auto get_min_max = [](const ColumnEstimator * estimator) -> std::optional<std::pair<Field, Field>>
    {
        if (!estimator)
            return std::nullopt;
        Estimate estimate = estimator->stats->getEstimate();
        if (!estimate.estimated_min || !estimate.estimated_max)
            return std::nullopt;
        return std::make_pair(*estimate.estimated_min, *estimate.estimated_max);
    };

    const auto lhs_min_max = get_min_max(lhs_estimator);
    const auto rhs_min_max = get_min_max(rhs_estimator);
    if (lhs_min_max && rhs_min_max)
    {
        Field lhs_min;
        Field lhs_max;
        Field rhs_min;
        Field rhs_max;
        if (tryConvertFieldLosslesslyToCommonType(lhs_min_max->first, lhs_type, common_type, lhs_min)
            && tryConvertFieldLosslesslyToCommonType(lhs_min_max->second, lhs_type, common_type, lhs_max)
            && tryConvertFieldLosslesslyToCommonType(rhs_min_max->first, rhs_type, common_type, rhs_min)
            && tryConvertFieldLosslesslyToCommonType(rhs_min_max->second, rhs_type, common_type, rhs_max))
        {
            const bool lhs_max_lt_rhs_min = Range::less(lhs_max, rhs_min);
            const bool rhs_max_lt_lhs_min = Range::less(rhs_max, lhs_min);
            const bool lhs_max_le_rhs_min = !Range::less(rhs_min, lhs_max);
            const bool rhs_max_le_lhs_min = !Range::less(lhs_min, rhs_max);

            if (function_name == "less")
            {
                if (lhs_max_lt_rhs_min)
                    set_finalized_selectivity(non_null_both, null_sel);
                else if (!Range::less(lhs_min, rhs_max))
                    set_finalized_selectivity(0.0, null_sel);
                else
                    set_finalized_selectivity(0.5 * non_null_both, null_sel);
                return true;
            }
            if (function_name == "lessOrEquals")
            {
                if (lhs_max_le_rhs_min)
                    set_finalized_selectivity(non_null_both, null_sel);
                else if (rhs_max_lt_lhs_min)
                    set_finalized_selectivity(0.0, null_sel);
                else
                    set_finalized_selectivity(0.5 * non_null_both, null_sel);
                return true;
            }
            if (function_name == "greater")
            {
                if (rhs_max_lt_lhs_min)
                    set_finalized_selectivity(non_null_both, null_sel);
                else if (lhs_max_le_rhs_min)
                    set_finalized_selectivity(0.0, null_sel);
                else
                    set_finalized_selectivity(0.5 * non_null_both, null_sel);
                return true;
            }
            if (function_name == "greaterOrEquals")
            {
                if (rhs_max_le_lhs_min)
                    set_finalized_selectivity(non_null_both, null_sel);
                else if (lhs_max_lt_rhs_min)
                    set_finalized_selectivity(0.0, null_sel);
                else
                    set_finalized_selectivity(0.5 * non_null_both, null_sel);
                return true;
            }
        }
    }

    /// No min/max proof available: use the standard overlapping-range heuristic on
    /// rows where both operands are non-NULL.
    set_finalized_selectivity(0.5 * non_null_both, null_sel);
    return true;
}

bool ConditionSelectivityEstimator::tryBuildColumnConstantAtom(
    const StorageMetadataPtr & metadata,
    const String & function_name,
    const String & column_name,
    Field const_value,
    DataTypePtr const_type,
    RPNElement & out) const
{
    if (!metadata || !isComparisonFunction(function_name))
        return false;

    const ColumnDescription * column_desc = metadata->getColumns().tryGet(column_name);
    if (!column_desc)
        return false;

    DataTypePtr column_type = removeLowCardinalityAndNullable(column_desc->type);

    /// Keep this conversion path aligned with the simple `column OP constant`
    /// extraction below: normalize the constant into the column domain only when
    /// doing so is conservative for statistics range estimation.
    bool cast_not_needed = !const_type
        || ((isNativeInteger(column_type) || isDateTime(column_type))
            && (isNativeInteger(const_type) || isDateTime(const_type)));

    if (!cast_not_needed && !column_type->equals(*const_type))
    {
        if (const_value.getType() == Field::Types::String)
        {
            try
            {
                const_value = convertFieldToType(const_value, *column_type);
            }
            catch (const Exception & e)
            {
                if (!isParseError(e.code()))
                    throw;

                if (function_name == "equals")
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }
                return false;
            }
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
                Field converted = tryConvertFieldToType(const_value, *common_type, const_type.get(), {});
                if (converted.isNull())
                    return false;

                const_value = converted;
            }
            if (!column_type->equals(*common_type))
            {
                if (!isFloat(column_type))
                    return false;

                Field converted = tryConvertFieldToType(const_value, *column_type, const_type.get(), {});
                if (converted.isNull())
                    return false;

                if (function_name == "equals" || function_name == "notEquals")
                {
                    Field round_trip = tryConvertFieldToType(converted, *common_type, column_type.get(), {});
                    if (round_trip.isNull() || round_trip != const_value)
                    {
                        out.function = function_name == "equals" ? RPNElement::ALWAYS_FALSE : RPNElement::ALWAYS_TRUE;
                        return true;
                    }
                }

                const_value = converted;
            }
        }
    }

    auto atom_it = atom_map.find(function_name);
    if (atom_it == atom_map.end())
        return false;

    atom_it->second(out, column_name, const_value);
    return true;
}

bool ConditionSelectivityEstimator::tryExtractTupleComparison(
    const StorageMetadataPtr & metadata,
    const String & function_name,
    const RPNBuilderTreeNode & lhs,
    const RPNBuilderTreeNode & rhs,
    RPNElement & out) const
{
    if (!metadata)
        return false;

    const bool is_in_operator = functionIsInOperator(function_name);
    const bool is_equality_operator = function_name == "equals" || function_name == "notEquals";
    if (!is_in_operator && !is_equality_operator)
        return false;

    auto collect_tuple_columns = [&](const RPNBuilderTreeNode & tuple_node, std::vector<String> & column_names) -> bool
    {
        if (!tuple_node.isFunction())
            return false;

        auto tuple_func = tuple_node.toFunctionNode();
        if (tuple_func.getFunctionName() != "tuple")
            return false;

        size_t tuple_size = tuple_func.getArgumentsSize();
        if (tuple_size < 2)
            return false;

        column_names.reserve(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            auto component = tuple_func.getArgumentAt(i);
            if (component.isConstant())
                return false;

            String column_name = component.getColumnName();
            const ColumnDescription * column_desc = metadata->getColumns().tryGet(column_name);
            if (!column_desc || isNullableOrLowCardinalityNullable(column_desc->type))
                return false;

            column_names.push_back(column_name);
        }

        return true;
    };

    std::vector<String> column_names;
    const RPNBuilderTreeNode * constant_tuple_node = &rhs;
    if (!collect_tuple_columns(lhs, column_names))
    {
        if (!is_equality_operator)
            return false;

        column_names.clear();
        if (!collect_tuple_columns(rhs, column_names))
            return false;
        constant_tuple_node = &lhs;
    }

    const size_t tuple_size = column_names.size();
    std::vector<Tuple> alternatives;

    auto add_tuple_alternative = [&](const Tuple & tuple_value) -> bool
    {
        if (tuple_value.size() != tuple_size)
            return false;
        alternatives.push_back(tuple_value);
        return true;
    };

    auto collect_from_constant_field = [&](const Field & field, bool in_operator) -> bool
    {
        if (field.getType() != Field::Types::Tuple)
            return false;

        const Tuple & tuple_value = field.safeGet<Tuple>();
        if (!in_operator)
            return add_tuple_alternative(tuple_value);

        if (tuple_value.empty())
            return true;

        /// Multi-column IN normally arrives as a tuple of tuple alternatives.
        /// Accept a single tuple of scalars too so `(a, b) IN tuple(1, 2)`-like
        /// folded forms can still be estimated safely.
        if (tuple_value.size() == tuple_size && tuple_value.front().getType() != Field::Types::Tuple)
            return add_tuple_alternative(tuple_value);

        for (const Field & alternative : tuple_value)
        {
            if (alternative.getType() != Field::Types::Tuple)
                return false;
            if (!add_tuple_alternative(alternative.safeGet<Tuple>()))
                return false;
        }
        return true;
    };

    auto collect_tuple_from_node = [&](const RPNBuilderTreeNode & tuple_node, Tuple & tuple_value) -> bool
    {
        Field field;
        DataTypePtr field_type;
        if (tuple_node.tryGetConstant(field, field_type))
        {
            if (field.getType() != Field::Types::Tuple)
                return false;
            tuple_value = field.safeGet<Tuple>();
            return tuple_value.size() == tuple_size;
        }

        if (!tuple_node.isFunction())
            return false;

        auto tuple_func = tuple_node.toFunctionNode();
        if (tuple_func.getFunctionName() != "tuple" || tuple_func.getArgumentsSize() != tuple_size)
            return false;

        tuple_value.resize(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            Field component;
            DataTypePtr component_type;
            if (!tuple_func.getArgumentAt(i).tryGetConstant(component, component_type))
                return false;
            tuple_value[i] = component;
        }
        return true;
    };

    auto collect_from_constant_node = [&](const RPNBuilderTreeNode & constant_node, bool in_operator) -> bool
    {
        Field field;
        DataTypePtr field_type;
        if (constant_node.tryGetConstant(field, field_type))
            return collect_from_constant_field(field, in_operator);

        if (!constant_node.isFunction())
            return false;

        auto tuple_func = constant_node.toFunctionNode();
        if (tuple_func.getFunctionName() != "tuple")
            return false;

        if (!in_operator)
        {
            Tuple tuple_value;
            return collect_tuple_from_node(constant_node, tuple_value) && add_tuple_alternative(tuple_value);
        }

        size_t before = alternatives.size();
        bool saw_tuple_alternative = false;
        for (size_t i = 0; i < tuple_func.getArgumentsSize(); ++i)
        {
            Tuple tuple_value;
            if (!collect_tuple_from_node(tuple_func.getArgumentAt(i), tuple_value))
                break;
            saw_tuple_alternative = true;
            if (!add_tuple_alternative(tuple_value))
                return false;
        }

        if (saw_tuple_alternative && alternatives.size() - before == tuple_func.getArgumentsSize())
            return true;

        alternatives.resize(before);
        Tuple tuple_value;
        return collect_tuple_from_node(constant_node, tuple_value) && add_tuple_alternative(tuple_value);
    };

    if (is_in_operator && !constant_tuple_node->getASTNode())
    {
        if (!constant_tuple_node->isConstant())
            return false;

        auto future_set = constant_tuple_node->tryGetPreparedSet();
        if (!future_set)
            return false;

        auto prepared_set = future_set->buildOrderedSetInplace(constant_tuple_node->getTreeContext().getQueryContext());
        if (!prepared_set || !prepared_set->hasExplicitSetElements())
            return false;

        Columns columns = prepared_set->getSetElements();
        if (columns.size() != tuple_size)
            return false;

        size_t rows = columns.empty() ? 0 : columns[0]->size();
        for (const auto & column : columns)
        {
            if (column->size() != rows)
                return false;
        }

        alternatives.reserve(rows);
        for (size_t row = 0; row < rows; ++row)
        {
            Tuple tuple_value(tuple_size);
            for (size_t column = 0; column < tuple_size; ++column)
                tuple_value[column] = (*columns[column])[row];
            alternatives.push_back(tuple_value);
        }
    }
    else
    {
        if (!collect_from_constant_node(*constant_tuple_node, is_in_operator))
            return false;
    }

    std::set<Tuple> distinct_tuples;
    for (Tuple tuple_value : alternatives)
    {
        Tuple converted_tuple(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            const Field & component_value = tuple_value[i];
            if (component_value.isNull())
                return false;

            RPNElement scalar_atom;
            DataTypePtr component_type = getFieldTypeForExactConversion(component_value, nullptr);
            if (!tryBuildColumnConstantAtom(metadata, "equals", column_names[i], component_value, component_type, scalar_atom))
                return false;

            /// Store the normalized range point so duplicate tuple alternatives are
            /// deduplicated after the same conversion used for estimation.
            if (scalar_atom.function == RPNElement::FUNCTION_IN_RANGE)
            {
                auto range_it = scalar_atom.column_ranges.find(column_names[i]);
                if (range_it == scalar_atom.column_ranges.end() || range_it->second.ranges.size() != 1)
                    return false;
                const Range & range = range_it->second.ranges.front();
                if (range.left != range.right)
                    return false;
                converted_tuple[i] = range.left;
            }
            else if (scalar_atom.function == RPNElement::ALWAYS_FALSE)
            {
                converted_tuple[i] = component_value;
            }
            else
                return false;
        }
        distinct_tuples.insert(converted_tuple);
    }

    Float64 in_selectivity = 0.0;
    for (const Tuple & tuple_value : distinct_tuples)
    {
        std::vector<std::pair<String, Field>> unique_column_constraints;
        bool tuple_impossible = false;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            RPNElement scalar_atom;
            DataTypePtr component_type = getFieldTypeForExactConversion(tuple_value[i], nullptr);
            if (!tryBuildColumnConstantAtom(metadata, "equals", column_names[i], tuple_value[i], component_type, scalar_atom))
                return false;

            if (scalar_atom.function == RPNElement::ALWAYS_FALSE)
            {
                tuple_impossible = true;
                break;
            }

            if (scalar_atom.function != RPNElement::FUNCTION_IN_RANGE)
                return false;
            auto range_it = scalar_atom.column_ranges.find(column_names[i]);
            if (range_it == scalar_atom.column_ranges.end() || range_it->second.ranges.size() != 1)
                return false;
            const Range & range = range_it->second.ranges.front();
            if (range.left != range.right)
                return false;

            bool found_existing_column = false;
            for (const auto & [existing_column, existing_value] : unique_column_constraints)
            {
                if (existing_column != column_names[i])
                    continue;
                found_existing_column = true;
                if (existing_value != range.left)
                    tuple_impossible = true;
                break;
            }
            if (tuple_impossible)
                break;
            if (!found_existing_column)
                unique_column_constraints.emplace_back(column_names[i], range.left);
        }

        if (tuple_impossible)
            continue;

        Float64 tuple_selectivity = 1.0;
        for (const auto & [column_name, value] : unique_column_constraints)
        {
            RPNElement scalar_atom;
            DataTypePtr component_type = getFieldTypeForExactConversion(value, nullptr);
            if (!tryBuildColumnConstantAtom(metadata, "equals", column_name, value, component_type, scalar_atom))
                return false;
            scalar_atom.finalize(column_estimators, metadata);
            tuple_selectivity *= scalar_atom.selectivity.true_sel;
        }

        RPNElement::TupleAlternative tuple_alternative;
        tuple_alternative.equalities = unique_column_constraints;
        tuple_alternative.selectivity = tuple_selectivity;
        out.tuple_alternatives.push_back(std::move(tuple_alternative));
        in_selectivity = std::min(1.0, in_selectivity + tuple_selectivity);
    }

    const bool is_negative = function_name == "notIn" || function_name == "notEquals";
    out.tuple_negated = is_negative;
    out.selectivity = Selectivity(is_negative ? 1.0 - in_selectivity : in_selectivity, 0.0);
    out.finalized = true;
    return true;
}

bool ConditionSelectivityEstimator::tryExtractExpressionDerivedRange(
    const StorageMetadataPtr & metadata,
    const String & function_name,
    const RPNBuilderTreeNode & expression,
    const Field & const_value,
    const DataTypePtr & const_type,
    RPNElement & out) const
{
    if (!metadata || !isComparisonFunction(function_name) || const_value.isNull())
        return false;

    auto try_get_cast_target_type = [](const RPNBuilderFunctionTreeNode & cast_func) -> DataTypePtr
    {
        if (const auto * dag_node = cast_func.getDAGNode())
            return removeLowCardinalityAndNullable(dag_node->result_type);

        if (cast_func.getArgumentsSize() < 2)
            return nullptr;

        Field type_name;
        DataTypePtr type_name_type;
        if (!cast_func.getArgumentAt(1).tryGetConstant(type_name, type_name_type) || type_name.getType() != Field::Types::String)
            return nullptr;

        try
        {
            return removeLowCardinalityAndNullable(DataTypeFactory::instance().get(type_name.safeGet<String>()));
        }
        catch (...)
        {
            return nullptr;
        }
    };

    auto convert_constant_through_cast = [&](
        const DataTypePtr & source_type,
        const DataTypePtr & target_type,
        Field & source_value,
        DataTypePtr & source_value_type) -> bool
    {
        DataTypePtr effective_const_type = getFieldTypeForExactConversion(const_value, const_type);
        if (!effective_const_type || !isNativeNumber(source_type) || !isNativeNumber(target_type) || !canBeSafelyCast(source_type, target_type))
            return false;

        Field target_value = const_value;
        if (!target_type->equals(*effective_const_type))
        {
            target_value = tryConvertFieldToType(const_value, *target_type, effective_const_type.get(), {}, /*strict=*/true);
            if (target_value.isNull())
                return false;

            Field round_trip = tryConvertFieldToType(target_value, *effective_const_type, target_type.get(), {}, /*strict=*/true);
            if (round_trip.isNull() || round_trip != const_value)
                return false;
        }

        source_value = tryConvertFieldToType(target_value, *source_type, target_type.get(), {}, /*strict=*/true);
        if (source_value.isNull())
            return false;

        Field round_trip = tryConvertFieldToType(source_value, *target_type, source_type.get(), {}, /*strict=*/true);
        if (round_trip.isNull() || round_trip != target_value)
            return false;

        source_value_type = source_type;
        return true;
    };

    auto try_extract_cast = [&]() -> bool
    {
        if (!expression.isFunction())
            return false;

        auto cast_func = expression.toFunctionNode();
        String cast_name = cast_func.getFunctionName();
        if (cast_name != "CAST" && cast_name != "_CAST")
            return false;
        if (cast_func.getArgumentsSize() < 1)
            return false;

        String column_name = cast_func.getArgumentAt(0).getColumnName();
        const ColumnDescription * column_desc = metadata->getColumns().tryGet(column_name);
        if (!column_desc)
            return false;

        DataTypePtr source_type = removeLowCardinalityAndNullable(column_desc->type);
        DataTypePtr target_type = try_get_cast_target_type(cast_func);
        if (!target_type)
            return false;

        Field source_value;
        DataTypePtr source_value_type;
        if (!convert_constant_through_cast(source_type, target_type, source_value, source_value_type))
            return false;

        return tryBuildColumnConstantAtom(metadata, function_name, column_name, source_value, source_value_type, out);
    };

    auto try_extract_arithmetic = [&]() -> bool
    {
        if (!expression.isFunction())
            return false;

        auto arithmetic_func = expression.toFunctionNode();
        String arithmetic_name = arithmetic_func.getFunctionName();
        if (arithmetic_name != "plus" && arithmetic_name != "minus")
            return false;
        if (arithmetic_func.getArgumentsSize() != 2 || !const_type)
            return false;

        size_t column_arg = 0;
        Field offset_value;
        DataTypePtr offset_type;
        if (arithmetic_func.getArgumentAt(1).tryGetConstant(offset_value, offset_type))
        {
            column_arg = 0;
        }
        else if (arithmetic_name == "plus" && arithmetic_func.getArgumentAt(0).tryGetConstant(offset_value, offset_type))
        {
            column_arg = 1;
        }
        else
            return false;

        const auto column_node = arithmetic_func.getArgumentAt(column_arg);
        String column_name = column_node.getColumnName();
        const ColumnDescription * column_desc = metadata->getColumns().tryGet(column_name);
        if (!column_desc)
            return false;

        DataTypePtr column_type = removeLowCardinalityAndNullable(column_desc->type);
        if (!isNativeInteger(column_type))
            return false;

        DataTypePtr effective_const_type = getFieldTypeForExactConversion(const_value, const_type);
        DataTypePtr effective_offset_type = getFieldTypeForExactConversion(offset_value, offset_type);
        if (!effective_const_type || !effective_offset_type
            || !isNativeInteger(effective_const_type) || !isNativeInteger(effective_offset_type))
            return false;

        Int64 const_int = 0;
        Int64 offset_int = 0;
        if (!tryConvertFieldToInt64Exactly(const_value, effective_const_type, const_int)
            || !tryConvertFieldToInt64Exactly(offset_value, effective_offset_type, offset_int))
            return false;

        auto estimator_it = column_estimators.find(column_name);
        if (estimator_it == column_estimators.end() || !isCompatibleStatistics(metadata, estimator_it->second.stats, column_name))
            return false;
        Estimate estimate = estimator_it->second.stats->getEstimate();
        if (!estimate.estimated_min || !estimate.estimated_max)
            return false;

        Int64 column_min = 0;
        Int64 column_max = 0;
        if (!tryConvertFieldToInt64Exactly(*estimate.estimated_min, column_type, column_min)
            || !tryConvertFieldToInt64Exactly(*estimate.estimated_max, column_type, column_max))
            return false;

        Int64 ignored = 0;
        if (arithmetic_name == "plus")
        {
            if (!addInt64Checked(column_min, offset_int, ignored) || !addInt64Checked(column_max, offset_int, ignored))
                return false;
        }
        else
        {
            if (!subtractInt64Checked(column_min, offset_int, ignored) || !subtractInt64Checked(column_max, offset_int, ignored))
                return false;
        }

        Int64 normalized_int = 0;
        if (arithmetic_name == "plus")
        {
            if (!subtractInt64Checked(const_int, offset_int, normalized_int))
                return false;
        }
        else
        {
            if (!addInt64Checked(const_int, offset_int, normalized_int))
                return false;
        }

        return tryBuildColumnConstantAtom(
            metadata,
            function_name,
            column_name,
            Field(normalized_int),
            std::make_shared<DataTypeInt64>(),
            out);
    };

    return try_extract_cast() || try_extract_arithmetic();
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
        auto atom_it = atom_map.find(func_name);
        if (atom_it == atom_map.end())
        {
            /// LIKE/ILIKE cannot be represented as a range. Pre-set selectivity
            /// so the estimator uses a tighter default than `default_unknown_cond_factor`.
            if (func_name == "like" || func_name == "ilike")
                out.selectivity.true_sel = default_like_factor;
            else if (func_name == "notLike" || func_name == "notILike")
                out.selectivity.true_sel = 1.0 - default_like_factor;
            else if (func_name == "__applyFilter")
            {
                /// Runtime join filter. Selectivity 1.0 keeps it last in prewhere ordering
                /// (after cheaper column predicates) and neutral for join reorder estimates.
                out.selectivity.true_sel = 1.0;
            }
            else
                return false;

            out.finalized = true;
            return false;
        }

        if (num_args == 1)
        {
            /// `isNull(col)` / `isNotNull(col)` — populate the corresponding null-check set.
            column_name = func.getArgumentAt(0).getColumnName();
            if (metadata && !metadata->getColumns().tryGet(column_name))
                return false;
            atom_it->second(out, column_name, Field{});
            return true;
        }

        if (num_args == 2)
        {
            if (tryExtractTupleComparison(metadata, func_name, func.getArgumentAt(0), func.getArgumentAt(1), out))
                return true;

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
                if (tryExtractExpressionDerivedRange(metadata, func_name, func.getArgumentAt(0), const_value, const_type, out))
                    return true;
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

                if (tryExtractExpressionDerivedRange(metadata, func_name, func.getArgumentAt(1), const_value, const_type, out))
                    return true;
            }
            else if (tryExtractColumnComparison(metadata, func_name, func.getArgumentAt(0), func.getArgumentAt(1), out))
                return true;
            else
                return false;

            if (metadata)
            {
                const ColumnDescription * column_desc = metadata->getColumns().tryGet(column_name);
                if (column_desc)
                    column_type = removeLowCardinalityAndNullable(column_desc->type);
                else
                {
                    /// Not a real column (e.g. a function expression like lower(col) or toDecimal64(col, 3)).
                    /// Skip range analysis to avoid bad cast when merging ranges of incompatible Field types.
                    /// Pre-set selectivity based on the function type so prewhere ordering is still reasonable.
                    if (func_name == "equals" || func_name == "in")
                        out.selectivity.true_sel = default_cond_equal_factor;
                    else if (func_name == "notEquals" || func_name == "notIn")
                        out.selectivity.true_sel = 1.0 - default_cond_equal_factor;
                    else /// less, greater, lessOrEquals, greaterOrEquals
                        out.selectivity.true_sel = default_cond_range_factor;
                    out.finalized = true;
                    return false;
                }
            }
            /// In some cases we need to cast the type of const
            bool cast_not_needed = !column_type || !const_type ||
                ((isNativeInteger(column_type) || isDateTime(column_type))
                && (isNativeInteger(const_type) || isDateTime(const_type)));

            if (!cast_not_needed && !column_type->equals(*const_type))
            {
                if (const_value.getType() == Field::Types::String)
                {
                    try
                    {
                        const_value = convertFieldToType(const_value, *column_type);
                    }
                    catch (const Exception & e)
                    {
                        if (!isParseError(e.code()))
                            throw;

                        /// The string value is not valid for the column type (e.g. unknown enum element).
                        /// For equality, the condition can never match, so selectivity is 0.
                        /// For other operators, fall back to default unknown selectivity.
                        LOG_DEBUG(getLogger("ConditionSelectivityEstimator"),
                            "Cannot convert value to column type, skipping statistics estimation. The exception is : {}",
                            getCurrentExceptionMessage(false));
                        if (func_name == "equals")
                        {
                            out.function = RPNElement::ALWAYS_FALSE;
                            return true;
                        }
                        return false;
                    }
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
                        /// The common type is wider than the column type (e.g. Float64 literal on a
                        /// Float32 column). For floating-point columns, narrow the constant to the
                        /// column type so that column statistics can be used.
                        /// For other combinations (e.g. Int32 column with Float64 constant) the
                        /// conversion semantics are non-trivial (ceiling vs. floor for range queries),
                        /// so we skip statistics estimation.
                        if (!isFloat(column_type))
                            return false;

                        Field converted = tryConvertFieldToType(const_value, *column_type, const_type.get(), {});
                        if (converted.isNull())
                            return false;

                        /// Narrowing to the column type is only semantically sound when the literal is
                        /// exactly representable in that type. At execution time the column value is
                        /// widened to the literal's type and compared against the original literal, not
                        /// against its narrowed value. For a non-representable literal (e.g. `f32 = 0.1`)
                        /// the comparison is always false, but narrowing would round the literal to a
                        /// representable value and make an impossible predicate look selective, which can
                        /// reorder PREWHERE incorrectly. Detect this with a round-trip and short-circuit
                        /// equality/inequality instead of estimating from the narrowed value.
                        if (func_name == "equals" || func_name == "notEquals")
                        {
                            Field round_trip = tryConvertFieldToType(converted, *common_type, column_type.get(), {});
                            if (round_trip.isNull() || round_trip != const_value)
                            {
                                out.function = func_name == "equals" ? RPNElement::ALWAYS_FALSE : RPNElement::ALWAYS_TRUE;
                                return true;
                            }
                        }

                        const_value = converted;
                    }
                }
            }

            /// The atom handlers for IN / NOT IN expect a Tuple but we may have parsed a single scalar in the case of IN (single_value).
            if (is_in_operator && const_value.getType() != Field::Types::Tuple)
                const_value = Tuple{const_value};

            atom_it = atom_map.find(func_name);
            atom_it->second(out, column_name, const_value);
            return true;
        }
    }

    /// Bare `<col>.null` UInt8 subcolumn reference, e.g. `SELECT … WHERE x.null`. Treat it as `IS NULL`.
    if (!node.isFunction() && !node.isConstant() && metadata)
    {
        String bare_column_name = node.getColumnName();

        auto dot_pos = bare_column_name.rfind('.');
        if (dot_pos != std::string::npos && bare_column_name.compare(dot_pos + 1, std::string::npos, "null") == 0)
        {
            String parent_name = bare_column_name.substr(0, dot_pos);
            const ColumnDescription * parent_col = metadata->getColumns().tryGet(parent_name);
            if (parent_col && isNullableOrLowCardinalityNullable(parent_col->type))
            {
                out.function = RPNElement::FUNCTION_IS_NULL;
                out.null_check_columns.insert(parent_name);
                return true;
            }
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

void ConditionSelectivityEstimatorBuilder::addStatistics(const String & column_name, const ColumnStatisticsPtr & column_stats)
{
    if (column_stats != nullptr)
    {
        has_data = true;
        auto & column_estimator = estimator->column_estimators[column_name];

        if (column_estimator.stats == nullptr)
            column_estimator.stats = column_stats;
        else if (column_estimator.stats->structureEquals(*column_stats))
            column_estimator.stats->merge(column_stats);
        /// else: incompatible statistics (e.g. a concurrent ALTER changed the column type,
        /// shifting the aggregate-function state layout). Skip this part's statistics so the
        /// estimator still works with the compatible parts instead of crashing.
    }
}

ConditionSelectivityEstimatorPtr ConditionSelectivityEstimatorBuilder::getEstimator() const
{
    return has_data ? estimator : nullptr;
}

ConditionSelectivityEstimator::Selectivity ConditionSelectivityEstimator::Selectivity::applyNot() const
{
    return {1.0 - true_sel - null_sel, null_sel};
}

ConditionSelectivityEstimator::Selectivity ConditionSelectivityEstimator::Selectivity::applyOr(const Selectivity & other) const
{
    /// case1: NULL or (FALSE/NULL) = NULL
    /// case2: FALSE or NULL = NULL
    /// case3: TRUE or (...) = TRUE
    /// case4: FALSE/NULL or TRUE = TRUE
    return {
        true_sel + (1 - true_sel) * other.true_sel,
        null_sel * (1 - other.true_sel) + (1 - null_sel - true_sel) * other.null_sel,
    };
}

ConditionSelectivityEstimator::Selectivity ConditionSelectivityEstimator::Selectivity::applyAnd(const Selectivity & other) const
{
    return {
        true_sel * other.true_sel,
        null_sel * (other.true_sel + other.null_sel) + true_sel * other.null_sel,
    };
}

ConditionSelectivityEstimator::Selectivity ConditionSelectivityEstimator::ColumnEstimator::estimateRanges(const PlainRanges & ranges) const
{
    if (stats->getNumRows() == 0)
        return {0, 0};
    Float64 result = 0;
    for (const Range & range : ranges.ranges)
    {
        if (auto estimate = stats->estimateRange(range))
            result += *estimate;
        else if (range.left == range.right)
            result += static_cast<Float64>(stats->getNonNullRowCount()) * default_cond_equal_factor;
        else
            result += static_cast<Float64>(stats->getNonNullRowCount()) * default_cond_range_factor;
    }
    Float64 rows = static_cast<Float64>(stats->getNumRows());
    Float64 selectivity = result / rows;
    /// Range predicates evaluate to NULL on NULL rows, so `true_sel` cannot exceed the
    /// non-NULL share of the column. Without this bound, summing estimates across many
    /// disjoint ranges (e.g. IN with many values) on a column with a large NULL share can
    /// produce `true_sel = 1, null_sel = 0.9`, leaving `false_sel = 1 - true_sel - null_sel`
    /// negative — which then breaks `applyAnd` / `applyOr` / `applyNot`.
    Float64 null_sel = static_cast<Float64>(stats->getNullCount()) / rows;
    Float64 non_null_share = std::max(0.0, 1.0 - null_sel);
    return {std::max(0.0, std::min(non_null_share, selectivity)), null_sel};
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
        },
        {
            "isNull",
            [] (RPNElement & out, const String & column, const Field &)
            {
                out.function = RPNElement::FUNCTION_IS_NULL;
                out.null_check_columns.insert(column);
            }
        },
        {
            "isNotNull",
            [] (RPNElement & out, const String & column, const Field &)
            {
                out.function = RPNElement::FUNCTION_IS_NOT_NULL;
                out.not_null_check_columns.insert(column);
            }
        }
};

/// merge CNF or DNF
bool ConditionSelectivityEstimator::RPNElement::tryToMergeClauses(RPNElement & lhs, RPNElement & rhs)
{
    auto can_merge_with = [](const RPNElement & e, Function function_to_merge)
    {
        return (e.function == FUNCTION_IN_RANGE
                || e.function == FUNCTION_IS_NULL
                || e.function == FUNCTION_IS_NOT_NULL
                /// if the sub-clause is also cnf/dnf, it's good to merge
                || e.function == function_to_merge
                /// if the sub-clause is different, but has only one column, it also works, e.g
                /// (a > 0 and a < 5) or (a > 3 and a < 10) can be merged to (a > 0 and a < 10)
                || (e.column_ranges.size() + e.column_not_ranges.size()
                    + e.null_check_columns.size() + e.not_null_check_columns.size()) == 1
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
    if (can_merge_with(lhs, function) && can_merge_with(rhs, function))
    {
        merge_column_ranges(column_ranges, lhs.column_ranges, rhs.column_ranges, false);
        merge_column_ranges(column_not_ranges, lhs.column_not_ranges, rhs.column_not_ranges, true);
        null_check_columns.insert(lhs.null_check_columns.begin(), lhs.null_check_columns.end());
        null_check_columns.insert(rhs.null_check_columns.begin(), rhs.null_check_columns.end());
        not_null_check_columns.insert(lhs.not_null_check_columns.begin(), lhs.not_null_check_columns.end());
        not_null_check_columns.insert(rhs.not_null_check_columns.begin(), rhs.not_null_check_columns.end());
        return true;
    }
    return false;
}

/// finalization of a expression means we would calculate the seletivity and no longer analyze ranges further.
void ConditionSelectivityEstimator::RPNElement::finalize(const ColumnEstimators & column_estimators_, const StorageMetadataPtr & metadata)
{
    if (finalized)
        return;

    finalized = true;

    if (function == FUNCTION_UNKNOWN)
    {
        selectivity = {default_unknown_cond_factor, 0};
        return;
    }

    if (function == ALWAYS_FALSE)
    {
        selectivity = Selectivity();
        return;
    }

    if (function == ALWAYS_TRUE)
    {
        selectivity = Selectivity(1.0, 0.0);
        return;
    }

    auto estimate_unknown_ranges = [&](const PlainRanges & ranges) -> Selectivity
    {
        Float64 equal_selectivity = 0;
        for (const Range & range : ranges.ranges)
        {
            if (range.isInfinite())
                return Selectivity{1.0, 0.0};

            if (range.left == range.right)
                equal_selectivity += default_cond_equal_factor;
            else
                return Selectivity{default_cond_range_factor, 0};
        }
        return Selectivity{std::min(equal_selectivity, 1.0), 0};
    };

    auto get_estimator = [&](const String & column_name) -> const ColumnEstimator *
    {
        auto it = column_estimators_.find(column_name);
        if (it == column_estimators_.end() || !isCompatibleStatistics(metadata, it->second.stats, column_name))
            return nullptr;
        return &it->second;
    };

    /// Per-column accumulator. The map enforces independence across columns: each column
    /// contributes a single `Selectivity` to the final AND/OR product, with intra-column
    /// merging (e.g. `col > 0 AND col IS NOT NULL`) folded in via `applyAnd`/`applyOr`.
    std::unordered_map<String, Selectivity> estimate_results;
    for (const auto & [column_name, ranges] : column_ranges)
    {
        if (const auto * est = get_estimator(column_name))
            estimate_results.emplace(column_name, est->estimateRanges(ranges));
        else
            estimate_results.emplace(column_name, estimate_unknown_ranges(ranges));
    }

    for (const auto & [column_name, ranges] : column_not_ranges)
    {
        Selectivity not_ranges_selectivity;
        if (const auto * est = get_estimator(column_name))
            not_ranges_selectivity = est->estimateRanges(ranges).applyNot();
        else
            not_ranges_selectivity = estimate_unknown_ranges(ranges).applyNot();

        auto it = estimate_results.find(column_name);
        if (it == estimate_results.end())
        {
            estimate_results.emplace(column_name, not_ranges_selectivity);
        }
        else if (function == FUNCTION_AND)
        {
            it->second = it->second.applyAnd(not_ranges_selectivity);
        }
        else /// FUNCTION_OR or FUNCTION_IN_RANGE
        {
            it->second = it->second.applyOr(not_ranges_selectivity);
        }
    }

    if (function == FUNCTION_AND)
    {
        /// `x IS NULL AND x IS NOT NULL` is a contradiction → selectivity = 0.
        /// Note: the previous explicit check for `x IS NULL AND (range on x)` is no longer
        /// needed — `applyAnd` over `Selectivity` reduces it to `true_sel = 0` automatically
        /// because range predicates have `true_sel = 0` on rows where the column is NULL.
        for (const auto & col : null_check_columns)
        {
            if (not_null_check_columns.contains(col))
            {
                selectivity = Selectivity();
                return;
            }
        }
    }
    else if (function == FUNCTION_OR)
    {
        /// `x IS NULL OR x IS NOT NULL` is a tautology → selectivity = 1
        for (const auto & col : null_check_columns)
        {
            if (not_null_check_columns.contains(col))
            {
                selectivity = Selectivity(1, 0);
                return;
            }
        }
    }

    for (const auto & column_name : null_check_columns)
    {
        Float64 cur_selectivity = default_cond_equal_factor;
        if (const auto * est = get_estimator(column_name))
            cur_selectivity = est->stats->estimateIsNull();

        if (!estimate_results.contains(column_name))
        {
            estimate_results.emplace(column_name, Selectivity{cur_selectivity, 0});
        }
        else if (function == FUNCTION_AND)
        {
            /// `x IS NULL AND <other predicate on x>`: any non-IS-NULL predicate on the same
            /// column has `true_sel = 0` on NULL rows, so the AND's `true_sel` is 0. The NULL
            /// share is `P(x IS NULL)` — IS NULL is TRUE there but the range predicate is NULL,
            /// so the AND evaluates to NULL on those rows. Store this per-column result so the
            /// final cross-column AND keeps folding in predicates on other columns.
            estimate_results[column_name] = Selectivity(0, cur_selectivity);
        }
        else
        {
            Float64 is_true = std::min(1.0, estimate_results[column_name].true_sel + cur_selectivity);
            estimate_results[column_name] = Selectivity(is_true, 0);
        }
    }

    for (const auto & column_name : not_null_check_columns)
    {
        Float64 cur_selectivity = 1.0 - default_cond_equal_factor;
        if (const auto * est = get_estimator(column_name))
            cur_selectivity = est->stats->estimateIsNotNull();

        if (!estimate_results.contains(column_name))
        {
            estimate_results.emplace(column_name, Selectivity{cur_selectivity, 0});
        }
        else if (function == FUNCTION_OR)
        {
            /// Under OR, `IS NOT NULL` dominates any range predicate on the same column,
            /// because non-NULL rows where the range is FALSE still satisfy IS NOT NULL.
            estimate_results[column_name].true_sel = cur_selectivity;
        }
        else
        {
            /// Under AND, the range predicate already filters NULLs; the only effect of
            /// `IS NOT NULL` is to zero out the column's NULL share.
            estimate_results[column_name].null_sel = 0;
        }
    }

    if (function == FUNCTION_OR)
        selectivity = Selectivity();
    else
        selectivity = Selectivity(1, 0);
    for (const auto & estimate_result : estimate_results)
    {
        if (function == FUNCTION_OR)
            selectivity = selectivity.applyOr(estimate_result.second);
        else
            selectivity = selectivity.applyAnd(estimate_result.second);
    }

    /// Clamp to valid probability range. Selectivity can exceed [0, 1] when
    /// estimateRanges() sums individual range estimates that together exceed the
    /// total row count (e.g. IN with many values and over-counting statistics),
    /// or become NaN from floating-point edge cases.
    if (!std::isfinite(selectivity.true_sel))
        selectivity.true_sel = default_unknown_cond_factor;
    else
        selectivity.true_sel = std::max(0.0, std::min(1.0, selectivity.true_sel));
}

}
