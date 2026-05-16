#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <stack>
#include <cmath>

#include <Common/logger_useful.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Processors/Formats/IRowInputFormat.h>


namespace DB
{

RelationProfile ConditionSelectivityEstimator::estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * filter, const ActionsDAG::Node * prewhere) const
{
    if (filter == nullptr && prewhere == nullptr)
        return estimateRelationProfile();
    if (filter == nullptr)
        return estimateRelationProfile(metadata, prewhere);
    if (prewhere == nullptr)
        return estimateRelationProfile(metadata, filter);

    auto extract_rpn = [&](const ActionsDAG::Node * node) -> std::vector<RPNElement>
    {
        return RPNBuilder<RPNElement>(node, getContext(), [&](const RPNBuilderTreeNode & node_, RPNElement & out)
        {
            return extractAtomFromTree(metadata, node_, out);
        }).extractRPN();
    };

    auto rpn = extract_rpn(filter);
    auto prewhere_rpn = extract_rpn(prewhere);
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
            column_name = func.getArgumentAt(0).getColumnName();
            if (metadata)
            {
                const auto * col = metadata->getColumns().tryGet(column_name);
                if (!col)
                    return false;
            }
            atom_it->second(out, column_name, Field{});
            return true;
        }

        else if (num_args == 2)
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
                        /// we assume that is "cast(column) < const", will not estimate this condition.
                        return false;
                    }
                }
            }

            /// The atom handlers for IN / NOT IN expect a Tuple but we may have parsed a single scalar in the case of IN (single_value).
            if (is_in_operator && const_value.getType() != Field::Types::Tuple)
                const_value = Tuple{const_value};

            atom_it->second(out, column_name, const_value);
            return true;
        }
    }

    if (!node.isFunction() && !node.isConstant() && metadata)
    {
        String bare_column_name = node.getColumnName();

        auto dot_pos = bare_column_name.rfind('.');
        if (dot_pos != std::string::npos)
        {
            String subcol_suffix = bare_column_name.substr(dot_pos + 1);
            String parent_name = bare_column_name.substr(0, dot_pos);

            if (subcol_suffix == "null")
            {
                const ColumnDescription * parent_col = metadata->getColumns().tryGet(parent_name);
                if (parent_col && isNullableOrLowCardinalityNullable(parent_col->type))
                {
                    out.function = RPNElement::FUNCTION_IS_NULL;
                    out.null_check_columns.insert(parent_name);
                    return true;
                }
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
        else
            column_estimator.stats->merge(column_stats);
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
    /// case1 : NULL or (false/NULL) = NULL
    /// case2 : false or NULL = NULL
    /// case3 : true or (...) = true
    /// case2 : false/NULL or true = true
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
    /// Clamp to [0, 1]. Selectivity can exceed 1 when summing estimates across
    /// multiple ranges (e.g. IN with many values) that together exceed total rows.
    return {std::max(0.0, std::min(1.0, selectivity)), static_cast<Float64>(stats->getNullCount()) / rows};
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
                    ranges.emplace_back(field);
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
                    ranges.emplace_back(field);
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

    auto estimate_unknown_ranges = [&](const PlainRanges & ranges)
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
        /// x IS NULL AND x IS NOT NULL is a contradiction → selectivity = 0
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
        /// x IS NULL OR x IS NOT NULL is a tautology → selectivity = 1
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
        Float64 cur_selectivity;
        if (const auto * est = get_estimator(column_name))
            cur_selectivity = est->stats->estimateIsNull();
        else
            cur_selectivity = default_cond_equal_factor;

        if (!estimate_results.contains(column_name))
        {
            estimate_results.emplace(column_name, Selectivity{cur_selectivity, 0});
        }
        else if (function == FUNCTION_AND)
        {
            selectivity = Selectivity();
            return;
        }
        else
        {
            Float64 is_true = std::min(1.0, estimate_results[column_name].true_sel + cur_selectivity);
            estimate_results[column_name] = Selectivity(is_true, 0);
        }
    }

    for (const auto & column_name : not_null_check_columns)
    {
        Float64 cur_selectivity;
        if (const auto * est = get_estimator(column_name))
            cur_selectivity = est->stats->estimateIsNotNull();
        else
            cur_selectivity = 1.0 - default_cond_equal_factor;

        if (!estimate_results.contains(column_name))
        {
            estimate_results.emplace(column_name, Selectivity{cur_selectivity, 0});
        }
        else if (function == FUNCTION_OR)
        {
            estimate_results[column_name].true_sel = cur_selectivity;
        }
        else
        {
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
