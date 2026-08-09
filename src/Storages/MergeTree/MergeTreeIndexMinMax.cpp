#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Analyzer/ConstantValue.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <numeric>

#include <Common/FieldAccurateComparison.h>
#include <Common/quoteString.h>

#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsBool use_minmax_index_bulk_filtering;
}


MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        serializations.push_back(type->getDefaultSerialization());
    }
    datatypes = index_sample_block.getDataTypes();
}

MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(
    const String & index_name_,
    const Block & index_sample_block_,
    Ranges && hyperrectangle_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , hyperrectangle(std::move(hyperrectangle_))
{
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        serializations.push_back(type->getDefaultSerialization());
    }
    datatypes = index_sample_block.getDataTypes();
}

void MergeTreeIndexGranuleMinMax::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty minmax index {}", backQuote(index_name));

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        serializations[i]->serializeBinary(hyperrectangle[i].left, ostr, {});
        serializations[i]->serializeBinary(hyperrectangle[i].right, ostr, {});
    }
}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    const size_t num_columns = index_sample_block.columns();

    /// On subsequent calls (when granule is reused), deserialize directly into the existing
    /// Range objects instead of clearing the vector and constructing new Ranges each time.
    /// This avoids repeated vector operations and Field copy-constructions
    /// in hot loops (e.g. skip index evaluation over hundreds of thousands of granules).
    const bool update_in_place = (hyperrectangle.size() == num_columns);

    if (!update_in_place)
        hyperrectangle.clear();

    Field min_val;
    Field max_val;

    for (size_t i = 0; i < num_columns; ++i)
    {
        /// When updating in place, deserialize directly into the Range's fields.
        Field & min_ref = update_in_place ? static_cast<Field &>(hyperrectangle[i].left) : min_val;
        Field & max_ref = update_in_place ? static_cast<Field &>(hyperrectangle[i].right) : max_val;

        switch (version)
        {
            case 1:
                if (!datatypes[i]->isNullable())
                {
                    serializations[i]->deserializeBinary(min_ref, istr, format_settings);
                    serializations[i]->deserializeBinary(max_ref, istr, format_settings);
                }
                else
                {
                    /// NOTE: that this serialization differs from
                    /// IMergeTreeDataPart::MinMaxIndex::load() to preserve
                    /// backward compatibility.
                    ///
                    /// But this is deprecated format, so this is OK.

                    bool is_null = false;
                    readBinary(is_null, istr);
                    if (!is_null)
                    {
                        serializations[i]->deserializeBinary(min_ref, istr, format_settings);
                        serializations[i]->deserializeBinary(max_ref, istr, format_settings);
                    }
                    else
                    {
                        min_ref = Null();
                        max_ref = Null();
                    }
                }
                break;

            /// New format with proper Nullable support for values that include NULL values
            case 2:
                serializations[i]->deserializeBinary(min_ref, istr, format_settings);
                serializations[i]->deserializeBinary(max_ref, istr, format_settings);

                // NULL_LAST
                if (min_ref.isNull())
                    min_ref = POSITIVE_INFINITY;
                if (max_ref.isNull())
                    max_ref = POSITIVE_INFINITY;

                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
        }

        if (update_in_place)
        {
            hyperrectangle[i].left_included = true;
            hyperrectangle[i].right_included = true;
        }
        else
        {
            hyperrectangle.emplace_back(min_val, true, max_val, true);
        }
    }
}

MergeTreeIndexAggregatorMinMax::MergeTreeIndexAggregatorMinMax(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorMinMax::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index_name, index_sample_block, std::move(hyperrectangle));
}

void MergeTreeIndexAggregatorMinMax::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    FieldRef field_min;
    FieldRef field_max;
    size_t range_start = *pos;
    size_t range_end = *pos + rows_read;
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        auto index_column_name = index_sample_block.getByPosition(i).name;
        const auto & src_column = block.getByName(index_column_name).column;
        /// Only LowCardinality needs unwrapping to expose a nested Nullable; gate the call so other
        /// columns are untouched. LC(Nullable(T)) then takes getExtremesNullLast (keeps the +inf NULL
        /// sentinel; otherwise IS NULL wrongly prunes). getExtremes on LC materializes internally too,
        /// so this adds no extra work.
        const auto column = src_column->lowCardinality() ? src_column->convertToFullColumnIfLowCardinality() : src_column;
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
            column_nullable->getExtremesNullLast(field_min, field_max, range_start, range_end);
        else
            column->getExtremes(field_min, field_max, range_start, range_end);

        if (hyperrectangle.size() <= i)
        {
            hyperrectangle.emplace_back(field_min, true, field_max, true);
        }
        else
        {
            hyperrectangle[i].left
                = accurateLess(hyperrectangle[i].left, field_min) ? hyperrectangle[i].left : field_min;
            hyperrectangle[i].right
                = accurateLess(hyperrectangle[i].right, field_max) ? field_max : hyperrectangle[i].right;
        }
    }

    *pos += rows_read;
}

namespace
{

constexpr auto OUTPUT_CAN_BE_TRUE = "__minmax_can_be_true";

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    const bool preserve_direct_comparisons = context->getSettingsRef()[Setting::use_minmax_index_bulk_filtering];
    return KeyCondition{
        filter_dag,
        context,
        index.column_names,
        index.expression,
        /* single_point_ = */ false,
        /* skip_analysis_ = */ false,
        preserve_direct_comparisons};
}

using Function = KeyCondition::RPNElement::Function;

String minMaxInputName(bool is_max, size_t column_index)
{
    return fmt::format("__minmax_{}_{}", is_max ? "max" : "min", column_index);
}

const ActionsDAG::Node & addConstUInt8(ActionsDAG & dag, UInt8 value, String name)
{
    auto type = std::make_shared<DataTypeUInt8>();
    auto column = type->createColumnConst(1, Field(value));
    return dag.addColumn(std::move(column), std::move(type), std::move(name));
}

const ActionsDAG::Node & addLiteral(ActionsDAG & dag, const ConstantValue & value, const String & name_hint)
{
    return dag.addColumn(value.getColumn(), value.getType(), name_hint);
}

const ActionsDAG::Node & addNamedFunction(ActionsDAG & dag, const String & fn_name, ActionsDAG::NodeRawConstPtrs children, ContextPtr context)
{
    auto resolver = FunctionFactory::instance().get(fn_name, context);
    return dag.addFunction(resolver, std::move(children), {});
}

/// Emit the two UInt8 nodes (intersects, contains) for a direct comparison atom.
std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>
buildIntersectsAndContains(
    ActionsDAG & dag,
    const KeyCondition::RPNElement & element,
    const DataTypes & index_data_types,
    const std::vector<std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>> & minmax_input_nodes,
    ContextPtr context)
{
    if (element.relaxed
        || !element.monotonic_functions_chain.empty()
        || element.bloom_filter_data.has_value()
        || element.argument_num_of_space_filling_curve.has_value()
        || element.key_columns.size() != 1
        || !element.direct_comparison)
        return {nullptr, nullptr};

    const size_t key_column = element.getKeyColumn();
    if (key_column >= index_data_types.size() || key_column >= minmax_input_nodes.size())
        return {nullptr, nullptr};

    const auto & min_node = *minmax_input_nodes[key_column].first;
    const auto & max_node = *minmax_input_nodes[key_column].second;
    const auto & comparison = *element.direct_comparison;
    const auto & literal = addLiteral(dag, *comparison.constant, fmt::format("__minmax_lit_{}", key_column));

    auto compare = [&](std::string_view name, const ActionsDAG::Node & value)
    {
        return &addNamedFunction(dag, String(name), {&value, &literal}, context);
    };

    auto lower_bound = [&](std::string_view name)
    {
        const ActionsDAG::Node * intersects = compare(name, max_node);
        const ActionsDAG::Node * contains = compare(name, min_node);

        /// SQL comparisons with NaN are unordered, unlike the scalar Range ordering.
        if (WhichDataType(removeLowCardinality(index_data_types[key_column])).isFloat())
        {
            const auto & max_is_nan = addNamedFunction(dag, "isNaN", {&max_node}, context);
            const auto & min_is_nan = addNamedFunction(dag, "isNaN", {&min_node}, context);
            const auto & max_is_finite = addNamedFunction(dag, "not", {&max_is_nan}, context);
            const auto & min_is_finite = addNamedFunction(dag, "not", {&min_is_nan}, context);
            const auto & mixed_finite_and_nan = addNamedFunction(dag, "and", {&max_is_nan, &min_is_finite}, context);
            intersects = &addNamedFunction(dag, "or", {intersects, &mixed_finite_and_nan}, context);
            contains = &addNamedFunction(dag, "and", {contains, &max_is_finite}, context);
        }
        return std::pair{intersects, contains};
    };

    using Operator = KeyCondition::RPNElement::DirectComparison::Operator;
    switch (comparison.op)
    {
        case Operator::Less:
            return {compare("less", min_node), compare("less", max_node)};
        case Operator::LessOrEquals:
            return {compare("lessOrEquals", min_node), compare("lessOrEquals", max_node)};
        case Operator::Greater:
            return lower_bound("greater");
        case Operator::GreaterOrEquals:
            return lower_bound("greaterOrEquals");
        case Operator::Equals:
        case Operator::NotEquals:
        {
            const auto [intersects_lower, contains_lower] = lower_bound("greaterOrEquals");
            const auto * intersects_upper = compare("lessOrEquals", min_node);
            const auto * contains_upper = compare("lessOrEquals", max_node);
            const auto & intersects = addNamedFunction(dag, "and", {intersects_upper, intersects_lower}, context);
            const auto & contains = addNamedFunction(dag, "and", {contains_upper, contains_lower}, context);
            return {&intersects, &contains};
        }
    }
    UNREACHABLE();
}

/// Lower the RPN into an ActionsDAG producing `can_be_true`, or return nullptr when an
/// element cannot be represented by direct typed comparisons.
ExpressionActionsPtr tryBuildMinMaxActions(
    const KeyCondition & key_condition,
    const DataTypes & index_data_types,
    const ContextPtr & context)
{
    const auto & rpn = key_condition.getRPN();
    if (rpn.empty())
        return nullptr;

    ActionsDAG dag;

    /// Nullable inputs produce nullable comparison masks and have a different serialized shape;
    /// leave them on the scalar path.
    std::vector<std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>> inputs;
    inputs.reserve(index_data_types.size());
    for (size_t i = 0; i < index_data_types.size(); ++i)
    {
        if (isNullableOrLowCardinalityNullable(index_data_types[i]))
            return nullptr;
        String min_name = minMaxInputName(false, i);
        String max_name = minMaxInputName(true, i);
        const auto & min_input = dag.addInput(min_name, index_data_types[i]);
        const auto & max_input = dag.addInput(max_name, index_data_types[i]);
        inputs.emplace_back(&min_input, &max_input);
    }

    /// Walk RPN, maintaining a stack of (can_be_true, can_be_false) node pairs.
    using NodePair = std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>;
    std::vector<NodePair> stack;
    stack.reserve(rpn.size());

    auto push_const = [&](bool v)
    {
        const auto & ctr = addConstUInt8(dag, v ? 1 : 0, v ? "const_true_ctr" : "const_false_ctr");
        const auto & cbf = addConstUInt8(dag, v ? 0 : 1, v ? "const_false_cbf" : "const_true_cbf");
        stack.emplace_back(&ctr, &cbf);
    };

    auto push_unknown = [&]()
    {
        const auto & t = addConstUInt8(dag, 1, "unknown_true");
        stack.emplace_back(&t, &t);
    };

    for (const auto & element : rpn)
    {
        switch (element.function)
        {
            case Function::FUNCTION_UNKNOWN:
                push_unknown();
                break;
            case Function::ALWAYS_TRUE:
                push_const(true);
                break;
            case Function::ALWAYS_FALSE:
                push_const(false);
                break;
            case Function::FUNCTION_IN_RANGE:
            case Function::FUNCTION_NOT_IN_RANGE:
            {
                auto [intersects, contains] = buildIntersectsAndContains(dag, element, index_data_types, inputs, context);
                if (!intersects || !contains)
                    return nullptr;
                const auto & cbf = addNamedFunction(dag, "not", {contains}, context);
                if (element.function == Function::FUNCTION_NOT_IN_RANGE)
                {
                    /// BoolMask negation: swap (ctr, cbf).
                    stack.emplace_back(&cbf, intersects);
                }
                else
                {
                    stack.emplace_back(intersects, &cbf);
                }
                break;
            }
            case Function::FUNCTION_AND:
            {
                if (stack.size() < 2)
                    return nullptr;
                auto rhs = stack.back(); stack.pop_back();
                auto lhs = stack.back(); stack.pop_back();
                const auto & ctr = addNamedFunction(dag, "and", {lhs.first, rhs.first}, context);
                const auto & cbf = addNamedFunction(dag, "or", {lhs.second, rhs.second}, context);
                stack.emplace_back(&ctr, &cbf);
                break;
            }
            case Function::FUNCTION_OR:
            {
                if (stack.size() < 2)
                    return nullptr;
                auto rhs = stack.back(); stack.pop_back();
                auto lhs = stack.back(); stack.pop_back();
                const auto & ctr = addNamedFunction(dag, "or", {lhs.first, rhs.first}, context);
                const auto & cbf = addNamedFunction(dag, "and", {lhs.second, rhs.second}, context);
                stack.emplace_back(&ctr, &cbf);
                break;
            }
            case Function::FUNCTION_NOT:
            {
                if (stack.empty())
                    return nullptr;
                auto top = stack.back();
                stack.back() = {top.second, top.first};
                break;
            }
            default:
                /// FUNCTION_IN_SET, FUNCTION_NOT_IN_SET, FUNCTION_IS_NULL, FUNCTION_IS_NOT_NULL,
                /// FUNCTION_ARGS_IN_HYPERRECTANGLE, FUNCTION_POINT_IN_POLYGON: not expressible here.
                return nullptr;
        }
    }

    if (stack.size() != 1)
        return nullptr;

    const auto & output = dag.addAlias(*stack.back().first, OUTPUT_CAN_BE_TRUE);
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&output);

    /// Allow expression compilation when enabled by settings.
    ExpressionActionsSettings expr_settings(context, CompileExpressions::yes);
    return std::make_shared<ExpressionActions>(std::move(dag), expr_settings);
}

}

MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
    : index_data_types(index.data_types)
    , condition(buildCondition(index, filter_dag, context))
{
    if (context->getSettingsRef()[Setting::use_minmax_index_bulk_filtering] && !alwaysUnknownOrTrue())
        minmax_actions = tryBuildMinMaxActions(condition, index_data_types, context);
}

bool MergeTreeIndexConditionMinMax::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        condition.getRPN(),
        {KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE,
         KeyCondition::RPNElement::FUNCTION_IN_RANGE,
         KeyCondition::RPNElement::FUNCTION_IN_SET,
         KeyCondition::RPNElement::FUNCTION_NOT_IN_SET,
         KeyCondition::RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE,
         KeyCondition::RPNElement::FUNCTION_POINT_IN_POLYGON,
         KeyCondition::RPNElement::FUNCTION_IS_NULL,
         KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL,
         KeyCondition::RPNElement::ALWAYS_FALSE});
}

bool MergeTreeIndexConditionMinMax::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const
{
    const MergeTreeIndexGranuleMinMax & granule = typeid_cast<const MergeTreeIndexGranuleMinMax &>(*idx_granule);
    return condition.checkInHyperrectangle(granule.hyperrectangle, index_data_types, {}, update_partial_disjunction_result_fn).can_be_true;
}

std::string MergeTreeIndexConditionMinMax::getDescription() const
{
    return condition.getDescription().condition;
}

MergeTreeIndexGranulePtr MergeTreeIndexMinMax::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexMinMax::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorMinMax>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexMinMax::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    ActionsDAGWithInversionPushDown filter_dag(predicate, context, /* boolean_context */ true);
    return std::make_shared<MergeTreeIndexConditionMinMax>(index, filter_dag, context);
}

MergeTreeIndexFormat MergeTreeIndexMinMax::getDeserializedFormat(const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const
{
    for (const auto & [column, _] : getColumnsWithTypesRequiredForIndexCalc())
        if (part.isSystemColumnInvalidated(column))
            return {0 /*unknown*/, {}};

    if (indexFileExistsInChecksums(part.checksums, relative_path_prefix, ".idx2", &part.getDataPartStorage()))
        return {2, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}};

    if (indexFileExistsInChecksums(part.checksums, relative_path_prefix, ".idx", &part.getDataPartStorage()))
        return {1, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}};

    return {0 /* unknown */, {}};
}

MergeTreeIndexSubstreams MergeTreeIndexMinMax::getAllSubstreamsInPart(
    const MergeTreeDataPartChecksums & checksums,
    const std::string & relative_path_prefix,
    const IDataPartStorage * storage) const
{
    /// minmax format changed `.idx` (v1) -> `.idx2` (v2); a part may carry both. Return every
    /// extension present, not just the preferred one, so cleanup does not miss the stale file.
    MergeTreeIndexSubstreams substreams;
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2", storage))
        substreams.push_back({MergeTreeIndexSubstream::Type::Regular, "", ".idx2"});
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx", storage))
        substreams.push_back({MergeTreeIndexSubstream::Type::Regular, "", ".idx"});
    return substreams;
}

MergeTreeIndexBulkGranulesMinMax::MergeTreeIndexBulkGranulesMinMax(const String & index_name_, const Block & index_sample_block_,
                                                                   size_t index_granularity_, int direction_, size_t size_hint_, size_t last_part_granule_, bool store_map_) :
    index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_granularity(index_granularity_)
    , direction(direction_)
    , last_part_granule(last_part_granule_)
    , store_map(store_map_)
{
    const DataTypePtr & type = index_sample_block.getByPosition(0).type;
    serialization = type->getDefaultSerialization();
    granules.reserve(size_hint_);
}

void MergeTreeIndexBulkGranulesMinMax::deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    Field value;
    Field scratch;

    /// The order in which values are read depends on 'direction':
    /// If direction == ASC, we need only min value, discard max value
    /// If direction == DESC, we need only max value, discard min value
    if (direction == 1)
    {
        serialization->deserializeBinary(value, istr, format_settings);
        serialization->deserializeBinary(scratch, istr, format_settings);
    }
    else
    {
        serialization->deserializeBinary(scratch, istr, format_settings);
        serialization->deserializeBinary(value, istr, format_settings);
    }
    /// If index granularity is not 1, we insert the same value as the min
    /// or max for all the corresponding granules. For our top-K purpose, this
    /// is safe and maybe lead to false positives, but never wrong results.
    for (size_t i = 0; i < index_granularity; ++i)
    {
        auto part_granule_num = (granule_num * index_granularity) + i;
        if (part_granule_num >= last_part_granule)
            break;

        granules.emplace_back(MinMaxGranule{part_granule_num, value});
        if (store_map)
            granules_map.emplace(part_granule_num, granules.size() - 1);
    }
    empty = false;
}

/// Get top K granules of a single part
template<bool handle_ties>
void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(size_t n, std::vector<MinMaxGranule> & result)
{
    if (n == 0)
        return;

    if (n >= granules.size())
    {
        result.insert(result.end(), granules.begin(), granules.end());
        return;
    }

    std::priority_queue<MinMaxGranuleItem> queue;

    for (const auto & granule : granules)
    {
        if constexpr (!handle_ties) /// more common case
        {
            if (queue.size() < n)
                queue.push({direction, 0, granule.granule_num, granule.min_or_max_value});
            else if ((direction == 1 && granule.min_or_max_value < queue.top().min_or_max_value) ||
                        (direction == -1 && granule.min_or_max_value > queue.top().min_or_max_value))
            {
                queue.pop();
                queue.push({direction, 0, granule.granule_num, granule.min_or_max_value});
            }
        }
        else
        {
            /// we need to return more than 'k' granules
            queue.push({-direction, 0, granule.granule_num, granule.min_or_max_value});
        }
    }

    if constexpr (!handle_ties)
    {
        while (!queue.empty())
        {
            result.push_back({queue.top().granule_num, queue.top().min_or_max_value});
            queue.pop();
        }
    }
    else
    {
        auto min_granules_to_select = n * index_granularity;
        auto threshold = queue.top();
        for (size_t i = 0; i < min_granules_to_select && !queue.empty(); ++i)
        {
            threshold = queue.top();
            result.push_back({queue.top().granule_num, queue.top().min_or_max_value});
            queue.pop();
        }

        while (!queue.empty() && queue.top().min_or_max_value == threshold.min_or_max_value)
        {
            result.push_back({queue.top().granule_num, queue.top().min_or_max_value});
            queue.pop();
        }
    }
}

void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(size_t n, bool handle_ties, std::vector<MinMaxGranule> & result)
{
    if (handle_ties)
        getTopKMarks<true>(n, result);
    else
        getTopKMarks<false>(n, result);
}

/// This routine is for top-N of top-N granules from all parts
template<bool handle_ties>
void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(int direction,
                                                    size_t n,
                                                    size_t index_granularity,
                                                    const std::vector<std::vector<MinMaxGranule>> & parts,
                                                    std::vector<MarkRanges> & result)
{
    if (n == 0)
        return;

    std::priority_queue<MinMaxGranuleItem> queue;

    for (size_t part_index = 0; part_index < parts.size(); ++part_index)
    {
        for (const auto & granule : parts[part_index])
        {
            if constexpr (!handle_ties) /// more common case
            {
                if (queue.size() < n)
                    queue.push({direction, part_index, granule.granule_num, granule.min_or_max_value});
                else if ((direction == 1 && granule.min_or_max_value < queue.top().min_or_max_value) ||
                            (direction == -1 && granule.min_or_max_value > queue.top().min_or_max_value))
                {
                    queue.pop();
                    queue.push({direction, part_index, granule.granule_num, granule.min_or_max_value});
                }
            }
            else
            {
                /// we need to return more than 'k' granules
                queue.push({-direction, part_index, granule.granule_num, granule.min_or_max_value});
            }
        }
    }

    if (queue.empty())
        return;

    result.resize(parts.size(), {});
    if constexpr (!handle_ties)
    {
        while (!queue.empty())
        {
            const auto & item = queue.top();
            result[item.part_index].push_back({item.granule_num, item.granule_num + 1});
            queue.pop();
        }
    }
    else
    {
        auto min_granules_to_select = n * index_granularity;
        auto threshold = queue.top();
        for (size_t i = 0; i < min_granules_to_select && !queue.empty(); ++i)
        {
            const auto & item = queue.top();
            threshold = queue.top();
            result[item.part_index].push_back({item.granule_num, item.granule_num + 1});
            queue.pop();
        }

        while (!queue.empty() && queue.top().min_or_max_value == threshold.min_or_max_value)
        {
            const auto & item = queue.top();
            result[item.part_index].push_back({item.granule_num, item.granule_num + 1});
            queue.pop();
        }
    }

    for (auto & part_ranges : result)
        std::sort(part_ranges.begin(), part_ranges.end());
}

void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(int direction,
                                                    size_t n,
                                                    size_t index_granularity,
                                                    bool handle_ties,
                                                    const std::vector<std::vector<MinMaxGranule>> & parts,
                                                    std::vector<MarkRanges> & result)
{
    if (handle_ties)
        getTopKMarks<true>(direction, n, index_granularity, parts, result);
    else
        getTopKMarks<false>(direction, n, index_granularity, parts, result);
}

namespace
{

/// Classify an index column's type for the native-width read fast path. Any Nullable type (or
/// wrapper that prefixes bytes onto the value) is excluded: the v1 format inserts a per-value
/// is_null byte, and v2 uses a Null-tagged Field for all-NULL granules. The slow path handles
/// both correctly; we only take the fast path when the on-disk layout is exactly a raw POD
/// ColumnVector<T> element per min / per max.
MergeTreeIndexBulkGranulesMinMaxColumnar::FastKind classifyFastKind(const IDataType & type)
{
    using FastKind = MergeTreeIndexBulkGranulesMinMaxColumnar::FastKind;
    if (type.isNullable())
        return FastKind::None;
    WhichDataType which(type);
    if (which.isUInt8())
        return FastKind::U8;
    if (which.isUInt16() || which.isDate())
        return FastKind::U16;
    if (which.isUInt32() || which.isDateTime())
        return FastKind::U32;
    if (which.isUInt64())
        return FastKind::U64;
    /// Enum8 stores values in `ColumnVector<Int8>`, so it must use the I8 fast path.
    /// Routing it under U8 caused `assert_cast<ColumnVector<UInt8>>` to throw at runtime.
    if (which.isInt8() || which.isEnum8())
        return FastKind::I8;
    if (which.isInt16() || which.isEnum16())
        return FastKind::I16;
    if (which.isInt32() || which.isDate32())
        return FastKind::I32;
    if (which.isInt64())
        return FastKind::I64;
    if (which.isFloat32())
        return FastKind::F32;
    if (which.isFloat64())
        return FastKind::F64;
    return FastKind::None;
}

template <typename T>
ALWAYS_INLINE void fastReadPair(IColumn & min_col, IColumn & max_col, ReadBuffer & istr)
{
    auto & min_data = assert_cast<ColumnVector<T> &>(min_col).getData();
    auto & max_data = assert_cast<ColumnVector<T> &>(max_col).getData();
    T raw;
    readPODBinary(raw, istr);
    min_data.push_back(raw);
    readPODBinary(raw, istr);
    max_data.push_back(raw);
}

}

MergeTreeIndexBulkGranulesMinMaxColumnar::MergeTreeIndexBulkGranulesMinMaxColumnar(const Block & index_sample_block)
    : datatypes(index_sample_block.getDataTypes())
{
    const size_t num_columns = index_sample_block.columns();
    serializations.reserve(num_columns);
    cols.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & type = index_sample_block.getByPosition(i).type;
        serializations.push_back(type->getDefaultSerialization());

        auto & column = cols[i];
        column.min_col = type->createColumn();
        column.max_col = type->createColumn();
        column.fast_kind = classifyFastKind(*type);
    }
}

void MergeTreeIndexBulkGranulesMinMaxColumnar::deserializeBinary(
    size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version)
{
    /// Rows are chunk-local, so `granule_num` is intentionally ignored.
    (void)granule_num;

    const size_t num_columns = cols.size();
    Field min_val;
    Field max_val;

    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & pc = cols[i];

        /// Fast path: the column is a fixed-width numeric, non-Nullable type. The on-disk
        /// serialization in v1 and v2 is the raw native bytes, so we can skip the `Field`
        /// round-trip and push straight into `ColumnVector<T>::getData()`.
        if (pc.fast_kind != FastKind::None)
        {
            switch (pc.fast_kind)
            {
                case FastKind::U8:  fastReadPair<UInt8>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::U16: fastReadPair<UInt16>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::U32: fastReadPair<UInt32>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::U64: fastReadPair<UInt64>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::I8:  fastReadPair<Int8>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::I16: fastReadPair<Int16>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::I32: fastReadPair<Int32>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::I64: fastReadPair<Int64>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::F32: fastReadPair<Float32>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::F64: fastReadPair<Float64>(*pc.min_col, *pc.max_col, istr); break;
                case FastKind::None: chassert(false); break;
            }
            continue;
        }

        /// Slow path: mirrors `MergeTreeIndexGranuleMinMax::deserializeBinary`.
        const auto & dtype = datatypes[i];
        switch (version)
        {
            case 1:
                if (!dtype->isNullable())
                {
                    serializations[i]->deserializeBinary(min_val, istr, format_settings);
                    serializations[i]->deserializeBinary(max_val, istr, format_settings);
                }
                else
                {
                    bool is_null = false;
                    readBinary(is_null, istr);
                    if (!is_null)
                    {
                        serializations[i]->deserializeBinary(min_val, istr, format_settings);
                        serializations[i]->deserializeBinary(max_val, istr, format_settings);
                    }
                    else
                    {
                        min_val = Null();
                        max_val = Null();
                    }
                }
                break;
            case 2:
                serializations[i]->deserializeBinary(min_val, istr, format_settings);
                serializations[i]->deserializeBinary(max_val, istr, format_settings);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
        }

        if (min_val.isNull())
            pc.min_col->insertDefault();
        else
            pc.min_col->insert(min_val);

        if (max_val.isNull())
            pc.max_col->insertDefault();
        else
            pc.max_col->insert(max_val);
    }

}

namespace
{

using ReadPairFn = void (*)(IColumn &, IColumn &, ReadBuffer &);

ReadPairFn readPairFunction(MergeTreeIndexBulkGranulesMinMaxColumnar::FastKind kind)
{
    using FastKind = MergeTreeIndexBulkGranulesMinMaxColumnar::FastKind;
    switch (kind)
    {
        case FastKind::U8:  return &fastReadPair<UInt8>;
        case FastKind::U16: return &fastReadPair<UInt16>;
        case FastKind::U32: return &fastReadPair<UInt32>;
        case FastKind::U64: return &fastReadPair<UInt64>;
        case FastKind::I8:  return &fastReadPair<Int8>;
        case FastKind::I16: return &fastReadPair<Int16>;
        case FastKind::I32: return &fastReadPair<Int32>;
        case FastKind::I64: return &fastReadPair<Int64>;
        case FastKind::F32: return &fastReadPair<Float32>;
        case FastKind::F64: return &fastReadPair<Float64>;
        case FastKind::None: return nullptr;
    }
    return nullptr;
}

}

void MergeTreeIndexBulkGranulesMinMaxColumnar::deserializeBinaryBulk(size_t count, ReadBuffer & istr, MergeTreeIndexVersion version)
{
    const size_t num_columns = cols.size();
    for (auto & column : cols)
    {
        column.min_col->reserve(column.min_col->size() + count);
        column.max_col->reserve(column.max_col->size() + count);
    }

    /// Resolve per-column fast-path function pointers once per chunk. For an index whose
    /// every column qualifies we avoid the switch-per-granule; for one that has any
    /// non-fast-path column we drop to the per-granule slow path.
    std::vector<ReadPairFn> fns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        fns[i] = readPairFunction(cols[i].fast_kind);
        if (!fns[i])
        {
            for (size_t g = 0; g < count; ++g)
                deserializeBinary(g, istr, version);
            return;
        }
    }

    /// The on-disk layout is interleaved by granule, so the granule loop stays outermost.
    for (size_t g = 0; g < count; ++g)
    {
        for (size_t i = 0; i < num_columns; ++i)
            fns[i](*cols[i].min_col, *cols[i].max_col, istr);
    }
}

MergeTreeIndexBulkGranulesPtr MergeTreeIndexMinMax::createIndexBulkGranules() const
{
    return std::make_shared<MergeTreeIndexBulkGranulesMinMaxColumnar>(index.sample_block);
}

IMergeTreeIndexCondition::FilteredGranules MergeTreeIndexConditionMinMax::getPossibleGranules(
    const MergeTreeIndexBulkGranulesPtr & idx_granules) const
{
    const auto & bulk = assert_cast<const MergeTreeIndexBulkGranulesMinMaxColumnar &>(*idx_granules);

    FilteredGranules all_granules(bulk.size());
    std::iota(all_granules.begin(), all_granules.end(), 0);
    if (!minmax_actions || index_data_types.size() != bulk.cols.size() || bulk.size() == 0)
        return all_granules;

    Block block;
    for (size_t i = 0; i < bulk.cols.size(); ++i)
    {
        const auto & type = index_data_types[i];
        block.insert(ColumnWithTypeAndName(bulk.cols[i].min_col->getPtr(), type, minMaxInputName(false, i)));
        block.insert(ColumnWithTypeAndName(bulk.cols[i].max_col->getPtr(), type, minMaxInputName(true, i)));
    }
    size_t num_rows = bulk.size();
    minmax_actions->execute(block, num_rows);
    const auto & can_be_true = block.getByName(OUTPUT_CAN_BE_TRUE).column;
    if (const auto * constant = typeid_cast<const ColumnConst *>(can_be_true.get()))
        return constant->getUInt(0) == 0 ? FilteredGranules{} : all_granules;

    const auto & data = assert_cast<const ColumnUInt8 &>(*can_be_true).getData();
    FilteredGranules out;
    out.reserve(data.size());
    for (size_t i = 0; i < data.size(); ++i)
        if (data[i])
            out.push_back(i);
    return out;
}

MergeTreeIndexPtr minmaxIndexCreator(
    StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & /*settings*/)
{
    return std::make_shared<MergeTreeIndexMinMax>(std::move(metadata_snapshot), index);
}

void minmaxIndexValidator(const IndexDescription & index, bool attach, const MergeTreeSettings & /*settings*/)
{
    if (attach)
        return;

    for (const auto & column : index.sample_block)
    {
        if (!column.type->isComparable())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Data type of argument for minmax index must be comparable, got {} type for column {} instead",
                column.type->getName(), column.name);
        }

        auto check_not_dynamic_or_variant = [&](const IDataType & type)
        {
            if (isDynamic(type) || isVariant(type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "{} data type of column {} is not allowed in minmax index because the values of that data type can contain values "
                    "with different data types. Consider using typed subcolumns or cast column to a specific data type",
                    column.type->getName(), column.name);
        };
        check_not_dynamic_or_variant(*column.type);
        column.type->forEachChild(check_not_dynamic_or_variant);
    }
}

}
