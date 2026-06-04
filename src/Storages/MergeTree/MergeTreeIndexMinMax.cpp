#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/MergeTree/BoolMask.h>

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
    std::vector<Range> && hyperrectangle_)
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
        const auto & column = block.getByName(index_column_name).column;
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

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    return KeyCondition{filter_dag, context, index.column_names, index.expression};
}

/// ============================================================================
/// ActionsDAG-backed fast path.
///
/// Lowers the KeyCondition RPN into a column-engine expression over paired
/// (min_c, max_c) columns per index column. At eval time the caller hands a
/// Block containing those columns and ClickHouse's standard function pipeline
/// produces two UInt8 output columns: `__minmax_can_be_true` and
/// `__minmax_can_be_false` per row (per granule).
///
/// When an RPN element can't be represented as plain column expressions
/// (monotonic function chains, space-filling curves, polygons, bloom filters,
/// relaxed predicates, non-collapsible set membership), the builder returns a
/// null `ExpressionActions` and the caller falls back to the generic
/// `Field`-based path.
/// ============================================================================

using Function = KeyCondition::RPNElement::Function;

String minMaxInputName(bool is_max, size_t column_index)
{
    return fmt::format("__minmax_{}_{}", is_max ? "max" : "min", column_index);
}

const ActionsDAG::Node & addConstUInt8(ActionsDAG & dag, UInt8 value, const String & name_hint = {})
{
    auto type = std::make_shared<DataTypeUInt8>();
    auto column = type->createColumnConst(1, Field(value));
    ColumnWithTypeAndName c{column, type, name_hint.empty() ? (value ? String("const_1") : String("const_0")) : name_hint};
    return dag.addColumn(std::move(c));
}

/// Add a literal (constant) column with the given DataType and value. The KeyCondition's range
/// bounds carry Fields whose underlying type does not always match the index column's
/// `DataType` - e.g. an `event_time DateTime64` index against `event_time >= now() - 600` ends
/// up with a `UInt64`-tagged Field bound while the column expects `Decimal64`. Run
/// `convertFieldToType` so the Field is reshaped to what `createColumnConst` requires;
/// returns `nullptr` (and the caller bails out of the bulk path) if no representable value
/// exists in the target type.
const ActionsDAG::Node * addLiteral(ActionsDAG & dag, const DataTypePtr & type, const Field & value, const String & name_hint)
{
    Field converted = convertFieldToType(value, *type);
    if (converted.isNull() && !value.isNull())
        return nullptr;
    auto column = type->createColumnConst(1, converted);
    ColumnWithTypeAndName c{column, type, name_hint};
    return &dag.addColumn(std::move(c));
}

const ActionsDAG::Node & addNamedFunction(ActionsDAG & dag, const String & fn_name, ActionsDAG::NodeRawConstPtrs children, ContextPtr context)
{
    auto resolver = FunctionFactory::instance().get(fn_name, context);
    return dag.addFunction(resolver, std::move(children), {});
}

/// Emit the two UInt8 nodes (intersects, contains) for a FUNCTION_IN_RANGE element. Returns
/// {nullptr, nullptr} if the element's shape isn't expressible (monotonic chain, bloom filter,
/// relaxed, space-filling curve, not single-column).
std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>
buildIntersectsAndContains(
    ActionsDAG & dag,
    const KeyCondition::RPNElement & element,
    const DataTypes & index_data_types,
    const std::vector<std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>> & minmax_input_nodes,
    ContextPtr context)
{
    if (element.relaxed)
        return {nullptr, nullptr};
    if (!element.monotonic_functions_chain.empty())
        return {nullptr, nullptr};
    if (element.bloom_filter_data.has_value())
        return {nullptr, nullptr};
    if (element.argument_num_of_space_filling_curve.has_value())
        return {nullptr, nullptr};
    if (element.key_columns.size() != 1)
        return {nullptr, nullptr};

    const size_t key_column = element.getKeyColumn();
    if (key_column >= index_data_types.size() || key_column >= minmax_input_nodes.size())
        return {nullptr, nullptr};

    const auto & min_node = *minmax_input_nodes[key_column].first;
    const auto & max_node = *minmax_input_nodes[key_column].second;
    const auto & column_type = index_data_types[key_column];

    const Range & range = element.range;
    const bool left_is_neg_inf = range.left.isNegativeInfinity();
    const bool right_is_pos_inf = range.right.isPositiveInfinity();
    /// "Inverted" infinities (left = +inf or right = -inf) shouldn't occur here; bail to be safe.
    if (range.left.isPositiveInfinity() || range.right.isNegativeInfinity())
        return {nullptr, nullptr};

    /// intersects: the element's range overlaps the granule's range.
    /// contains:   the granule's range is fully inside the element's range.
    const ActionsDAG::Node * intersects_upper = nullptr;
    const ActionsDAG::Node * intersects_lower = nullptr;
    const ActionsDAG::Node * contains_upper = nullptr;
    const ActionsDAG::Node * contains_lower = nullptr;

    if (right_is_pos_inf)
    {
        intersects_upper = &addConstUInt8(dag, 1, "const_true");
        contains_upper = &addConstUInt8(dag, 1, "const_true");
    }
    else
    {
        const auto * right_lit = addLiteral(dag, column_type, range.right, fmt::format("__minmax_lit_right_{}", key_column));
        if (!right_lit)
            return {nullptr, nullptr};
        intersects_upper = &addNamedFunction(dag, range.right_included ? "lessOrEquals" : "less", {&min_node, right_lit}, context);
        contains_upper = &addNamedFunction(dag, range.right_included ? "lessOrEquals" : "less", {&max_node, right_lit}, context);
    }

    if (left_is_neg_inf)
    {
        intersects_lower = &addConstUInt8(dag, 1, "const_true");
        contains_lower = &addConstUInt8(dag, 1, "const_true");
    }
    else
    {
        const auto * left_lit = addLiteral(dag, column_type, range.left, fmt::format("__minmax_lit_left_{}", key_column));
        if (!left_lit)
            return {nullptr, nullptr};
        intersects_lower = &addNamedFunction(dag, range.left_included ? "greaterOrEquals" : "greater", {&max_node, left_lit}, context);
        contains_lower = &addNamedFunction(dag, range.left_included ? "greaterOrEquals" : "greater", {&min_node, left_lit}, context);

        /// Mirror the `NaN` semantics of `KeyCondition::checkInHyperrectangle`. A Float granule that
        /// mixes finite values with `NaN` is stored as `[finite_min, NaN]`, because `NaN` sorts last.
        /// The scalar path keeps such a granule for a lower-bounded predicate: it forces `contains`
        /// to false but leaves `intersects` true, because the finite values may still match. Here
        /// `greaterOrEquals(NaN, left)` is false, which would wrongly prune the granule, so treat a
        /// `NaN` granule max as reaching any finite lower bound. `contains_lower` uses the granule min
        /// (finite in this case) and `contains_upper` (granule max) already evaluates to false against
        /// `NaN`, so `contains` stays false as the scalar path requires.
        /// Unwrap `LowCardinality` first: a non-nullable `LowCardinality(Float)` minmax index still
        /// reaches this path, but `WhichDataType(LowCardinality(Float64)).isFloat()` is false, which
        /// would leave the `NaN` fix inactive and wrongly prune `[finite_min, NaN]` granules.
        if (WhichDataType(removeLowCardinality(column_type)).isFloat())
        {
            const auto & max_is_nan = addNamedFunction(dag, "isNaN", {&max_node}, context);
            intersects_lower = &addNamedFunction(dag, "or", {intersects_lower, &max_is_nan}, context);
        }
    }

    const auto & intersects = addNamedFunction(dag, "and", {intersects_upper, intersects_lower}, context);
    const auto & contains = addNamedFunction(dag, "and", {contains_upper, contains_lower}, context);
    return {&intersects, &contains};
}

/// Try to lower the entire RPN into an ActionsDAG. On success returns non-null ExpressionActions
/// with two outputs (can_be_true, can_be_false) and fills `minmax_input_names` with per-index-column
/// (min_name, max_name) pairs so the evaluator knows what columns to populate.
/// On failure (any unsupported RPN element), returns nullptr.
ExpressionActionsPtr tryBuildMinMaxActions(
    const KeyCondition & key_condition,
    const DataTypes & index_data_types,
    const ContextPtr & context,
    std::vector<std::pair<String, String>> & minmax_input_names_out,
    const char * output_ctr_name,
    const char * output_cbf_name)
{
    const auto & rpn = key_condition.getRPN();
    if (rpn.empty())
        return nullptr;

    ActionsDAG dag;

    /// One pair of input nodes per index column. Nullable index columns are excluded:
    /// (1) the v1 on-disk format prefixes each value with an is-null byte and the v2 format
    ///     uses a Null-tagged Field for all-NULL granules, neither of which the bulk
    ///     deserializer currently consumes correctly;
    /// (2) even if deserialization worked, `less(Nullable(T), Nullable-literal)` returns
    ///     `Nullable(UInt8)` and the final `and` of those stays Nullable, while the caller
    ///     asserts a plain `ColumnUInt8` on the output.
    /// This also covers `LowCardinality(Nullable(T))`: a comparison over it still yields
    /// `Nullable(UInt8)`, so checking `isNullable` alone would let such a type slip through
    /// and trip the `ColumnUInt8` assertion at evaluation time.
    /// Falling back to the generic per-granule path is correct and preserves NULL semantics.
    std::vector<std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>> inputs;
    inputs.reserve(index_data_types.size());
    minmax_input_names_out.clear();
    minmax_input_names_out.reserve(index_data_types.size());
    for (size_t i = 0; i < index_data_types.size(); ++i)
    {
        if (isNullableOrLowCardinalityNullable(index_data_types[i]))
            return nullptr;
        String min_name = minMaxInputName(false, i);
        String max_name = minMaxInputName(true, i);
        const auto & min_input = dag.addInput(min_name, index_data_types[i]);
        const auto & max_input = dag.addInput(max_name, index_data_types[i]);
        inputs.emplace_back(&min_input, &max_input);
        minmax_input_names_out.emplace_back(std::move(min_name), std::move(max_name));
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

    const auto & ctr_alias = dag.addAlias(*stack.back().first, output_ctr_name);
    const auto & cbf_alias = dag.addAlias(*stack.back().second, output_cbf_name);
    dag.getOutputs().clear();
    dag.getOutputs().push_back(&ctr_alias);
    dag.getOutputs().push_back(&cbf_alias);

    /// Request JIT compilation: after `min_count_to_compile_expression` (default 3) invocations
    /// of the same DAG shape, the CompiledExpressionCache fuses the compare+AND chain into a
    /// single LLVM-generated function and caches it. The cache is process-wide and keyed by a
    /// hash of the DAG, so all queries with the same condition shape reuse the compiled kernel.
    /// Honors the `compile_expressions` session setting; if that's off, the constructor
    /// silently skips compilation.
    ExpressionActionsSettings expr_settings(context, CompileExpressions::yes);
    return std::make_shared<ExpressionActions>(std::move(dag), expr_settings);
}

}

MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
    : index_data_types(index.data_types)
    , condition(buildCondition(index, filter_dag, context))
{
    /// Lower the RPN into an ActionsDAG over paired (min, max) columns. When this succeeds,
    /// the bulk path can evaluate all granules at once via the column engine, avoiding all
    /// Field-variant dispatch. `minmax_actions` stays null for unsupported RPN shapes
    /// (monotonic chains, space-filling curves, polygons, bloom filters, relaxed predicates,
    /// non-collapsed IN_SET); in that case the caller falls back to the generic path.
    ///
    /// The condition object is constructed during query analysis on every read of every
    /// minmax-indexed table. When `use_minmax_index_bulk_filtering` is off (the default), the
    /// bulk path is never taken, so building the DAG would be wasted query-analysis work; skip
    /// it entirely and leave `minmax_actions` null, making the disabled setting a true no-op.
    ///
    /// An unexpected Field/Type combination that escapes the explicit eligibility checks
    /// would otherwise fail the whole query; the per-granule scalar path is correct on
    /// every shape, so the safe fallback is to leave `minmax_actions` null.
    if (context->getSettingsRef()[Setting::use_minmax_index_bulk_filtering])
    {
        try
        {
            minmax_actions = tryBuildMinMaxActions(condition, index_data_types, context, minmax_input_names,
                                                   OUTPUT_CAN_BE_TRUE, OUTPUT_CAN_BE_FALSE);
        }
        catch (...)
        {
            /// Ok: see comment above; the per-granule scalar path is the safe fallback.
            minmax_actions = nullptr;
            minmax_input_names.clear();
        }
    }
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
    ActionsDAGWithInversionPushDown filter_dag(predicate, context);
    return std::make_shared<MergeTreeIndexConditionMinMax>(index, filter_dag, context);
}

MergeTreeIndexFormat MergeTreeIndexMinMax::getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2"))
        return {2, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}};
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx"))
        return {1, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}};
    return {0 /* unknown */, {}};
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

/// Is a Field `Null` with the positive-infinity flag set? v2 minmax maps all-NULL granules to
/// POSITIVE_INFINITY (NULL_LAST semantics); we preserve that marker as a per-granule flag.
bool isPosInfNull(const Field & f)
{
    return f.getType() == Field::Types::Null && f.isPositiveInfinity();
}

bool isNegInfNull(const Field & f)
{
    return f.getType() == Field::Types::Null && f.isNegativeInfinity();
}

}

namespace
{

/// Classify an index column's type for the native-width read fast path. Any Nullable type (or
/// wrapper that prefixes bytes onto the value) is excluded: the v1 format inserts a per-value
/// is_null byte, and v2 uses a Null-tagged Field for all-NULL granules. The slow path handles
/// both correctly; we only take the fast path when the on-disk layout is exactly a raw POD
/// ColumnVector<T> element per min / per max.
MergeTreeIndexBulkGranulesMinMaxFast::FastKind classifyFastKind(const IDataType & type)
{
    using FastKind = MergeTreeIndexBulkGranulesMinMaxFast::FastKind;
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

MergeTreeIndexBulkGranulesMinMaxFast::MergeTreeIndexBulkGranulesMinMaxFast(
    const Block & index_sample_block_, size_t size_hint)
    : index_sample_block(index_sample_block_)
{
    const size_t num_columns = index_sample_block.columns();
    datatypes = index_sample_block.getDataTypes();
    serializations.reserve(num_columns);
    cols.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & type = index_sample_block.getByPosition(i).type;
        serializations.push_back(type->getDefaultSerialization());

        auto & pc = cols[i];
        /// Always materialize a column of the index column's exact type. The ActionsDAG-based
        /// bulk path needs the type to match the DAG's inputs; any type that `IColumn::insert`
        /// accepts for Field values works here.
        pc.min_col = type->createColumn();
        pc.max_col = type->createColumn();
        pc.min_col->reserve(size_hint);
        pc.max_col->reserve(size_hint);
        /// inf flag arrays are only populated on the slow path; skip the reserve when we know
        /// we'll never touch them.
        pc.fast_kind = classifyFastKind(*type);
        if (pc.fast_kind == FastKind::None)
        {
            pc.min_is_neg_inf.reserve(size_hint);
            pc.min_is_pos_inf.reserve(size_hint);
            pc.max_is_neg_inf.reserve(size_hint);
            pc.max_is_pos_inf.reserve(size_hint);
        }
    }
}

void MergeTreeIndexBulkGranulesMinMaxFast::deserializeBinary(
    size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version)
{
    /// `granule_num` is the caller's running counter. For the current bulk API it matches `size`,
    /// but we don't rely on that: we just append in order and let `getPossibleGranules` return
    /// row indices (which are the granule numbers the caller used).
    (void)granule_num;

    const size_t num_columns = index_sample_block.columns();
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
                if (min_val.isNull())
                    min_val = POSITIVE_INFINITY;
                if (max_val.isNull())
                    max_val = POSITIVE_INFINITY;
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
        }

        const bool min_pos_inf = isPosInfNull(min_val);
        const bool min_neg_inf = isNegInfNull(min_val);
        const bool max_pos_inf = isPosInfNull(max_val);
        const bool max_neg_inf = isNegInfNull(max_val);
        pc.min_is_pos_inf.push_back(static_cast<UInt8>(min_pos_inf));
        pc.min_is_neg_inf.push_back(static_cast<UInt8>(min_neg_inf));
        pc.max_is_pos_inf.push_back(static_cast<UInt8>(max_pos_inf));
        pc.max_is_neg_inf.push_back(static_cast<UInt8>(max_neg_inf));

        if (min_pos_inf || min_neg_inf)
            pc.min_col->insertDefault();
        else
            pc.min_col->insert(min_val);

        if (max_pos_inf || max_neg_inf)
            pc.max_col->insertDefault();
        else
            pc.max_col->insert(max_val);
    }

    ++size;
}

namespace
{

/// Per-column helper that appends one (min, max) pair read via `readPODBinary` to its columns.
/// Resolved at deserializeBinaryBulk() entry so the type-dispatch happens once per chunk (not
/// once per granule or once per (granule, column) pair).
template <typename T>
ALWAYS_INLINE void fastReadOnePair(IColumn & min_col, IColumn & max_col, ReadBuffer & istr)
{
    auto & min_data = assert_cast<ColumnVector<T> &>(min_col).getData();
    auto & max_data = assert_cast<ColumnVector<T> &>(max_col).getData();
    T raw;
    readPODBinary(raw, istr);
    min_data.push_back(raw);
    readPODBinary(raw, istr);
    max_data.push_back(raw);
}

using FastReadOnePairFn = void (*)(IColumn &, IColumn &, ReadBuffer &);

FastReadOnePairFn fastReadOnePairFn(MergeTreeIndexBulkGranulesMinMaxFast::FastKind kind)
{
    using FastKind = MergeTreeIndexBulkGranulesMinMaxFast::FastKind;
    switch (kind)
    {
        case FastKind::U8:  return &fastReadOnePair<UInt8>;
        case FastKind::U16: return &fastReadOnePair<UInt16>;
        case FastKind::U32: return &fastReadOnePair<UInt32>;
        case FastKind::U64: return &fastReadOnePair<UInt64>;
        case FastKind::I8:  return &fastReadOnePair<Int8>;
        case FastKind::I16: return &fastReadOnePair<Int16>;
        case FastKind::I32: return &fastReadOnePair<Int32>;
        case FastKind::I64: return &fastReadOnePair<Int64>;
        case FastKind::F32: return &fastReadOnePair<Float32>;
        case FastKind::F64: return &fastReadOnePair<Float64>;
        case FastKind::None: return nullptr;
    }
    return nullptr;
}

}

void MergeTreeIndexBulkGranulesMinMaxFast::deserializeBinaryBulk(size_t count, ReadBuffer & istr, MergeTreeIndexVersion version)
{
    const size_t num_columns = index_sample_block.columns();

    /// Resolve per-column fast-path function pointers once per chunk. For an index whose
    /// every column qualifies we avoid the switch-per-granule; for one that has any
    /// non-fast-path column we drop to the per-granule slow path.
    std::vector<FastReadOnePairFn> fns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        fns[i] = fastReadOnePairFn(cols[i].fast_kind);
        if (!fns[i])
        {
            for (size_t g = 0; g < count; ++g)
                deserializeBinary(size + g, istr, version);
            return;
        }
    }

    /// Reserve once for the whole chunk, then loop. On-disk layout is per-granule
    /// interleaved (granule0 = col0_min col0_max col1_min col1_max ..., then granule1,
    /// ...), so the outer loop has to be over granules with the column loop inside;
    /// bulk reads per-column would stride non-contiguously through the stream.
    for (size_t i = 0; i < num_columns; ++i)
    {
        cols[i].min_col->reserve(cols[i].min_col->size() + count);
        cols[i].max_col->reserve(cols[i].max_col->size() + count);
    }

    for (size_t g = 0; g < count; ++g)
    {
        for (size_t i = 0; i < num_columns; ++i)
            fns[i](*cols[i].min_col, *cols[i].max_col, istr);
    }
    size += count;
}

MergeTreeIndexBulkGranulesPtr MergeTreeIndexMinMax::createIndexBulkGranules() const
{
    return std::make_shared<MergeTreeIndexBulkGranulesMinMaxFast>(index.sample_block, /*size_hint=*/1024);
}

IMergeTreeIndexCondition::FilteredGranules MergeTreeIndexConditionMinMax::getPossibleGranules(
    const MergeTreeIndexBulkGranulesPtr & idx_granules) const
{
    const auto & bulk = assert_cast<const MergeTreeIndexBulkGranulesMinMaxFast &>(*idx_granules);
    const size_t n = bulk.size;

    FilteredGranules all_granules;
    all_granules.resize(n);
    for (size_t i = 0; i < n; ++i)
        all_granules[i] = i;

    if (n == 0)
    {
        FilteredGranules empty;
        return empty;
    }

    /// Preferred path: delegate the RPN evaluation to the column engine. `minmax_actions` was
    /// built at condition-construction time by lowering the RPN into operations over paired
    /// (min, max) columns. We just assemble a Block with those columns and execute.
    if (minmax_actions && minmax_input_names.size() == bulk.cols.size())
    {
        /// Build a block with the (min_c, max_c) columns for each index column. The bulk
        /// container holds these as MutableColumnPtr; we hand the Block a shared ColumnPtr
        /// view via getPtr() so the bulk container's ownership isn't disturbed.
        Block block;
        for (size_t i = 0; i < minmax_input_names.size(); ++i)
        {
            const auto & type = index_data_types[i];
            const IColumn & min_ref = *bulk.cols[i].min_col;
            const IColumn & max_ref = *bulk.cols[i].max_col;
            block.insert(ColumnWithTypeAndName(min_ref.getPtr(), type, minmax_input_names[i].first));
            block.insert(ColumnWithTypeAndName(max_ref.getPtr(), type, minmax_input_names[i].second));
        }

        size_t num_rows = n;
        minmax_actions->execute(block, num_rows);

        const auto & ctr_col_ref = block.getByName(OUTPUT_CAN_BE_TRUE).column;

        /// Tautologically-true / tautologically-false conditions collapse the output to a
        /// `ColumnConst`. Short-circuit those: skip materializing an n-row mask we already
        /// know the value of.
        if (const auto * const_col = typeid_cast<const ColumnConst *>(ctr_col_ref.get()))
        {
            if (const_col->getUInt(0) == 0)
                return {};
            return all_granules;
        }

        const auto & ctr_col = assert_cast<const ColumnUInt8 &>(*ctr_col_ref).getData();

        FilteredGranules out;
        out.reserve(ctr_col.size());
        for (size_t i = 0; i < ctr_col.size(); ++i)
            if (ctr_col[i])
                out.push_back(i);
        return out;
    }

    /// No DAG available and no other bulk path. Return `all_granules` to indicate "no pruning
    /// from this index" (the caller then treats the index as a pass-through). The caller should
    /// have filtered out this case via `hasBulkFastPath()` and skipped bulk entirely, so this
    /// branch is mostly defensive.
    return all_granules;
}

MergeTreeIndexPtr minmaxIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexMinMax>(index);
}

void minmaxIndexValidator(const IndexDescription & index, bool attach)
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

        if (isDynamic(column.type) || isVariant(column.type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{} data type of column {} is not allowed in minmax index because the column of that type can contain values with different data "
                "types. Consider using typed subcolumns or cast column to a specific data type",
                column.type->getName(), column.name);
        }
    }
}

}
