#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <algorithm>
#include <cmath>
#include <stack>

#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Formats/ParseError.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/likePatternToRegexp.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace
{

enum class LikePatternKind
{
    Unsupported,
    MatchEverything,
    Exact,
    Prefix,
    Suffix,
    Contains,
};

struct LikePatternClassification
{
    LikePatternKind kind = LikePatternKind::Unsupported;
    String literal;
};

bool isLikeFunctionName(const String & func_name)
{
    return func_name == "like" || func_name == "ilike" || func_name == "notLike" || func_name == "notILike";
}

bool isNegatedLikeFunctionName(const String & func_name)
{
    return func_name == "notLike" || func_name == "notILike";
}

LikePatternClassification classifyLikePattern(std::string_view pattern)
{
    String current_literal;
    current_literal.reserve(pattern.size());
    std::vector<String> literal_segments;

    bool starts_with_percent = false;
    bool ends_with_percent = false;
    bool in_percent_run = false;
    bool saw_anything = false;
    size_t percent_runs = 0;

    auto finish_literal_segment = [&]()
    {
        if (!current_literal.empty())
        {
            literal_segments.push_back(current_literal);
            current_literal.clear();
        }
    };

    const char * pos = pattern.data();
    const char * const end = pattern.data() + pattern.size();
    while (pos < end)
    {
        switch (*pos)
        {
            case '%': {
                if (!in_percent_run)
                {
                    ++percent_runs;
                    if (!saw_anything)
                        starts_with_percent = true;
                    finish_literal_segment();
                    in_percent_run = true;
                }
                ends_with_percent = true;
                saw_anything = true;
                ++pos;
                break;
            }
            case '_': return {};
            case '\\': {
                in_percent_run = false;
                ends_with_percent = false;
                saw_anything = true;
                ++pos;
                if (pos == end)
                    return {};

                /// Match likePatternToRegexp(): only %, _ and \\ are special escape sequences.
                /// For an unknown escape sequence the backslash is literal and the following
                /// character is processed normally on the next iteration.
                if (*pos == '%' || *pos == '_' || *pos == '\\')
                {
                    current_literal += *pos;
                    ++pos;
                }
                else
                {
                    current_literal += '\\';
                }
                break;
            }
            default: {
                in_percent_run = false;
                ends_with_percent = false;
                saw_anything = true;
                current_literal += *pos;
                ++pos;
                break;
            }
        }
    }
    finish_literal_segment();

    if (percent_runs == 0)
        return {LikePatternKind::Exact, literal_segments.empty() ? String{} : literal_segments.front()};

    if (literal_segments.empty())
        return {LikePatternKind::MatchEverything, {}};

    if (literal_segments.size() != 1)
        return {};

    if (!starts_with_percent && ends_with_percent && percent_runs == 1)
        return {LikePatternKind::Prefix, literal_segments.front()};

    if (starts_with_percent && !ends_with_percent && percent_runs == 1)
        return {LikePatternKind::Suffix, literal_segments.front()};

    if (starts_with_percent && ends_with_percent && percent_runs == 2)
        return {LikePatternKind::Contains, literal_segments.front()};

    return {};
}

Float64
estimateLikePatternHeuristic(LikePatternKind kind, size_t literal_size, Float64 default_like_factor, Float64 default_cond_equal_factor)
{
    if (kind == LikePatternKind::Exact)
        return default_cond_equal_factor;

    const Float64 capped_size = static_cast<Float64>(std::min<size_t>(literal_size, 4));
    Float64 selectivity = default_like_factor;
    if (kind == LikePatternKind::Prefix)
        selectivity = std::pow(0.25, capped_size);
    else if (kind == LikePatternKind::Suffix)
        selectivity = std::pow(0.35, capped_size);
    else if (kind == LikePatternKind::Contains)
        selectivity = std::pow(0.45, capped_size);

    /// Keep these pattern-shape estimates conservative: never larger than the legacy LIKE
    /// fallback and never so tiny that a short pattern becomes indistinguishable from zero.
    return std::max(0.001, std::min(default_like_factor, selectivity));
}

}

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
                    for (auto & selectivity_by_column : last_element->column_selectivities)
                        selectivity_by_column.second = selectivity_by_column.second.applyNot();
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
        auto try_extract_like_atom_from_tree = [&]() -> bool
        {
            if (num_args != 2 && num_args != 3)
                return false;

            if (!metadata)
                return false;

            String like_column_name = func.getArgumentAt(0).getColumnName();
            const ColumnDescription * column_desc = metadata->getColumns().tryGet(like_column_name);
            if (!column_desc)
                return false;

            DataTypePtr like_column_type = removeLowCardinalityAndNullable(column_desc->type);
            if (!isStringOrFixedString(like_column_type))
                return false;
            const auto * fixed_string_type = typeid_cast<const DataTypeFixedString *>(like_column_type.get());

            Field pattern_value;
            DataTypePtr pattern_type;
            if (!func.getArgumentAt(1).tryGetConstant(pattern_value, pattern_type))
                return false;

            if (pattern_value.isNull())
            {
                /// `x LIKE NULL` and `x NOT LIKE NULL` evaluate to SQL NULL for every row, so WHERE
                /// sees zero TRUE rows. Keep the NULL state finalized so outer NOT preserves it.
                out.selectivity = Selectivity{0.0, 1.0};
                out.finalized = true;
                return true;
            }

            if (pattern_value.getType() != Field::Types::String)
                return false;

            String pattern = pattern_value.safeGet<String>();
            if (num_args == 3)
            {
                Field escape_value;
                DataTypePtr escape_type;
                if (!func.getArgumentAt(2).tryGetConstant(escape_value, escape_type))
                    return false;
                if (escape_value.isNull() || escape_value.getType() != Field::Types::String)
                    return false;

                const String & escape_string = escape_value.safeGet<String>();
                if (escape_string.size() != 1 || static_cast<unsigned char>(escape_string[0]) > 0x7F)
                    return false;

                try
                {
                    pattern = likePatternWithCustomEscapeToLikePattern(pattern, escape_string[0]);
                }
                catch (const Exception &)
                {
                    return false;
                }
            }

            LikePatternClassification classification = classifyLikePattern(pattern);
            if (classification.kind == LikePatternKind::Unsupported)
                return false;

            const bool negated = isNegatedLikeFunctionName(func_name);

            auto get_null_share = [&]() -> Float64
            {
                if (!isNullableOrLowCardinalityNullable(column_desc->type))
                    return 0.0;

                auto it = column_estimators.find(like_column_name);
                if (it != column_estimators.end() && isCompatibleStatistics(metadata, it->second.stats, like_column_name))
                    return std::max(0.0, std::min(1.0, it->second.stats->estimateIsNull()));

                return default_cond_equal_factor;
            };

            auto add_column_selectivity = [&](Float64 positive_true_sel)
            {
                Float64 null_sel = get_null_share();
                Float64 non_null_share = std::max(0.0, 1.0 - null_sel);
                positive_true_sel = std::max(0.0, std::min(non_null_share, positive_true_sel));

                Selectivity selectivity{positive_true_sel, null_sel};
                if (negated)
                    selectivity = selectivity.applyNot();

                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_selectivities.emplace(like_column_name, selectivity);
            };

            switch (classification.kind)
            {
                case LikePatternKind::Exact: {
                    /// Case-sensitive LIKE with no wildcards is equivalent to equality for String, so
                    /// reuse the normal range/equality machinery there. ILIKE is only exact syntactically;
                    /// semantically it is case-insensitive, so use an equality-like per-column heuristic.
                    /// Exact LIKE on FixedString can only match when the byte length matches
                    /// FixedString(N). Equality ignores trailing NUL padding for shorter String
                    /// literals, while LIKE matches all N bytes; investigate padding-aware
                    /// FixedString LIKE estimates in a follow-up.
                    if (fixed_string_type && classification.literal.size() != fixed_string_type->getN())
                    {
                        add_column_selectivity(0.0);
                        return true;
                    }

                    if (func_name == "like" || func_name == "notLike")
                    {
                        out.function = RPNElement::FUNCTION_IN_RANGE;
                        if (negated)
                            out.column_not_ranges.emplace(like_column_name, Range(Field(classification.literal)));
                        else
                            out.column_ranges.emplace(like_column_name, Range(Field(classification.literal)));
                    }
                    else
                    {
                        add_column_selectivity(estimateLikePatternHeuristic(
                            classification.kind, classification.literal.size(), default_like_factor, default_cond_equal_factor));
                    }
                    return true;
                }
                case LikePatternKind::MatchEverything: add_column_selectivity(1.0); return true;
                case LikePatternKind::Prefix:
                case LikePatternKind::Suffix:
                case LikePatternKind::Contains:
                    add_column_selectivity(estimateLikePatternHeuristic(
                        classification.kind, classification.literal.size(), default_like_factor, default_cond_equal_factor));
                    return true;
                case LikePatternKind::Unsupported: return false;
            }

            return false;
        };

        auto atom_it = atom_map.find(func_name);
        if (atom_it == atom_map.end())
        {
            if (isLikeFunctionName(func_name) && try_extract_like_atom_from_tree())
                return true;

            /// LIKE/ILIKE cannot always be represented as a range. Pre-set selectivity
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
    auto merge_column_selectivities = [this](
                                          std::unordered_map<String, Selectivity> & result,
                                          const std::unordered_map<String, Selectivity> & lhs_selectivities,
                                          const std::unordered_map<String, Selectivity> & rhs_selectivities)
    {
        for (const auto & [column_name, lhs_selectivity] : lhs_selectivities)
        {
            auto rhs_it = rhs_selectivities.find(column_name);
            if (rhs_it == rhs_selectivities.end())
            {
                result.emplace(column_name, lhs_selectivity);
                continue;
            }

            const Selectivity & rhs_selectivity = rhs_it->second;
            Float64 null_sel = std::max(lhs_selectivity.null_sel, rhs_selectivity.null_sel);
            Float64 true_sel = 0.0;
            if (function == FUNCTION_AND)
                true_sel = std::min(lhs_selectivity.true_sel, rhs_selectivity.true_sel);
            else
                true_sel = std::min(std::max(0.0, 1.0 - null_sel), lhs_selectivity.true_sel + rhs_selectivity.true_sel);

            result.emplace(column_name, Selectivity{true_sel, null_sel});
        }
        for (const auto & [column_name, rhs_selectivity] : rhs_selectivities)
        {
            if (!lhs_selectivities.contains(column_name))
                result.emplace(column_name, rhs_selectivity);
        }
    };

    if (can_merge_with(lhs, function) && can_merge_with(rhs, function))
    {
        merge_column_ranges(column_ranges, lhs.column_ranges, rhs.column_ranges, false);
        merge_column_ranges(column_not_ranges, lhs.column_not_ranges, rhs.column_not_ranges, true);
        merge_column_selectivities(column_selectivities, lhs.column_selectivities, rhs.column_selectivities);
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
    /// merging (e.g. `col > 0 AND col IS NOT NULL`) folded in before the final product.
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

    for (const auto & [column_name, like_selectivity] : column_selectivities)
    {
        auto it = estimate_results.find(column_name);
        if (it == estimate_results.end())
        {
            estimate_results.emplace(column_name, like_selectivity);
        }
        else if (function == FUNCTION_AND)
        {
            it->second.true_sel = std::min(it->second.true_sel, like_selectivity.true_sel);
            it->second.null_sel = std::max(it->second.null_sel, like_selectivity.null_sel);
        }
        else /// FUNCTION_OR or FUNCTION_IN_RANGE
        {
            Float64 null_sel = std::max(it->second.null_sel, like_selectivity.null_sel);
            it->second.true_sel = std::min(std::max(0.0, 1.0 - null_sel), it->second.true_sel + like_selectivity.true_sel);
            it->second.null_sel = null_sel;
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
