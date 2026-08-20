#include <Columns/ColumnConst.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Compression/CompressionCodecQuantized.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationQuantizedVector.h>
#include <Common/Exception.h>
#include <Common/VectorQuantizer.h>
#include <Functions/IFunction.h>
#include <Functions/productQuantization.h>
#include <Functions/vectorQuantization.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <ranges>
#include <unordered_set>

namespace DB::Setting
{
    extern const SettingsFloat vector_search_index_fetch_multiplier;
    extern const SettingsBool vector_search_use_quantized_codes;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}
}

namespace DB::QueryPlanOptimizations
{

/// "Quantized-codes" brute-force vector search optimization.
///
/// This is the brute-force counterpart of the vector-similarity-index optimization in useVectorSearch.cpp. It targets
/// tables that do NOT have a vector similarity index but DO have a vector column carrying a `Quantized(...)` codec, which
/// stores a compact quantized companion stream exposed as the subcolumn `<column>.quantized`. The codes are tiny (e.g.
/// 12 bytes) compared to the full-precision vector (e.g. 960 * 4 bytes), so we can rank cheaply over the codes first and
/// only read the heavy vector for the survivors.
///
/// For a query of the form
///     SELECT [...] FROM tab WHERE [...] ORDER BY L2Distance(vec, ref) LIMIT k
/// we rewrite the plan into a two-stage "shortlist then rescore":
///
///     LimitStep (k)                                            [outer: final top-k]
///       SortingStep (L2Distance(vec, ref) ASC)
///         ExpressionStep (_distance := L2Distance(vec, ref))   [rescore against full precision]
///           LimitStep (k')                                     [inner: shortlist, k' = k * fetch_multiplier]
///             SortingStep (_approx ASC)
///               ExpressionStep (_approx := quantizeDistance(vec.quantized, ref, ...))  [approximate, codes only]
///                 (FilterStep)                                 [the WHERE/PREWHERE predicate]
///                   ReadFromMergeTree (..., vec.quantized)     [reads the small codes subcolumn]
///
/// We do NOT defer the heavy vector column here. The general lazy-materialization optimization
/// (optimizeLazyMaterialization2) runs after this pass and fires on the inner LimitStep (k' <=
/// query_plan_max_limit_for_lazy_materialization), automatically reading `vec` only for the k' shortlisted rows. The
/// outer stage then rescores those k' rows against the full-precision vector and returns the exact top-k.
namespace
{

/// If the storage column `search_column` carries a `Quantize(...)` codec, return its parameters.
std::optional<QuantizedCodecParams> findQuantizeCodecParams(const ReadFromMergeTree & read_step, const String & search_column)
{
    for (const auto & column : read_step.getStorageMetadata()->getColumns())
        if (column.name == search_column)
            return tryExtractQuantizedCodecParams(column.codec);
    return {};
}

}

/// Brute-force vector search over a vector column carrying a `Quantized(...)` codec: rewrite ORDER BY distance LIMIT
/// into a two-stage shortlist (over the quantized codes subcolumn) + rescore (against the full-precision vector).
bool optimizeVectorSearchWithQuantizedCodes(
    QueryPlan::Node & /*root*/, Stack & stack, QueryPlan::Nodes & nodes,
    const Optimization::ExtraSettings & settings, size_t max_limit_for_lazy_materialization)
{
    /// Expect this query plan (same shape as the index optimization's first pass):
    /// LimitStep -> SortingStep -> ExpressionStep -> (optional FilterStep) -> ReadFromMergeTree

    QueryPlan::Node * limit_node = stack.back().node;
    auto * limit_step = typeid_cast<LimitStep *>(limit_node->step.get());
    if (!limit_step || limit_node->children.size() != 1)
        return false;

    QueryPlan::Node * sorting_node = limit_node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting_step || sorting_step->getType() != SortingStep::Type::Full || sorting_node->children.size() != 1)
        return false;

    QueryPlan::Node * expression_node = sorting_node->children.front();
    auto * expression_step = typeid_cast<ExpressionStep *>(expression_node->step.get());
    if (!expression_step || expression_node->children.size() != 1)
        return false;

    /// Descend through the chain of Expression/Filter steps between the rescore expression and the read until we reach
    /// ReadFromMergeTree. The chain holds the WHERE predicate - either as a FilterStep, or (when moved to PREWHERE) as
    /// a rename ExpressionStep with the actual filter inside the reader. We collect the chain so we can splice the
    /// shortlist ABOVE all of it: this way every filter prefilters the approximate ranking. The codes subcolumn is then
    /// propagated up through the chain (the steps would otherwise drop it as an unknown column).
    std::vector<QueryPlan::Node *> chain_nodes; /// ordered top (just below the rescore expression) to bottom (just above read)
    QueryPlan::Node * read_node = expression_node->children.front();
    ReadFromMergeTree * read_step = nullptr;
    while (true)
    {
        read_step = typeid_cast<ReadFromMergeTree *>(read_node->step.get());
        if (read_step)
            break;
        if (!typeid_cast<ExpressionStep *>(read_node->step.get()) && !typeid_cast<FilterStep *>(read_node->step.get()))
            return false;
        if (read_node->children.size() != 1)
            return false;
        chain_nodes.push_back(read_node);
        read_node = read_node->children.front();
    }

    /// Leave the vector-similarity-index path alone: if the first pass claimed this read for the index, or it is a
    /// distributed read, do not engage the codes optimization. (PREWHERE is fine - it prefilters inside the reader.)
    if (read_step->getVectorSearchParameters().has_value())
        return false;
    if (read_step->isParallelReadingFromReplicas())
        return false;

    /// The two-stage rewrite (shortlist over quantized codes + exact rescore) is an approximate search, so it is opt-in:
    /// by default an ORDER BY distance LIMIT on a Quantize-coded column runs as an exact full-precision scan. Only engage
    /// when the user asks for it via `vector_search_use_quantized_codes`.
    if (!read_step->getContext()->getSettingsRef()[Setting::vector_search_use_quantized_codes])
        return false;

    /// The shortlist uses internal functions (`__quantizeDistance`/`__productQuantizationDistance`) that are not registered in
    /// FunctionFactory, so a remote node could not deserialize the plan. Leave the query exact when the plan is
    /// distributed (the vector-search-index path is skipped for the same reason above).
    if (settings.make_distributed_plan)
        return false;

    /// Number of rows the final top-k needs (includes any OFFSET).
    const size_t n = limit_step->getLimitForSorting();
    if (n == 0 || n > settings.max_limit_for_vector_search_queries)
        return false;

    /// The inner shortlist LimitStep is plain. WITH TIES (rows tied with the boundary distance) and read-till-end
    /// (totals / exact_rows_before_limit) semantics would be violated if the approximate stage dropped those rows
    /// before the original limit sees them, so do not rewrite in those cases.
    if (limit_step->withTies() || limit_step->alwaysReadTillEnd())
        return false;

    /// Read the ORDER BY clause: it must be a single distance_function(vec, reference_vector).
    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.size() != 1)
        return false;
    const String & sort_column = sort_description.front().column_name;

    ActionsDAG & expression = expression_step->getExpression();
    /// A row-changing action (e.g. arrayJoin) in this expression expands rows before the sort/limit. Splicing the inner
    /// shortlist limit below it would cut rows before the expansion and change the result cardinality, so bail out.
    if (expression.hasArrayJoin())
        return false;
    const ActionsDAG::Node * sort_column_node = expression.tryFindInOutputs(sort_column);
    if (sort_column_node == nullptr || sort_column_node->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    const String & function_name = sort_column_node->function_base->getName();
    /// dotProduct is intentionally unsupported: the codes path ranks ascending by a distance.
    if (function_name != "L2Distance" && function_name != "cosineDistance")
        return false;
    const int sort_direction = sort_description.front().direction;
    if (sort_direction != 1)
        return false;
    const bool is_l2 = function_name == "L2Distance";

    /// Extract the search column and the reference vector from distance_function(vec, [reference_vector]).
    VectorWithMemoryTracking<Float64> reference_vector;
    String search_column;
    for (const auto * child : sort_column_node->children)
    {
        if (child->type == ActionsDAG::ActionType::ALIAS)
        {
            const auto * search_column_node = child->children.at(0);
            if (search_column_node->type == ActionsDAG::ActionType::INPUT)
                search_column = search_column_node->result_name;
        }
        else if (child->type == ActionsDAG::ActionType::INPUT)
        {
            search_column = child->result_name;
        }
        else if (child->type == ActionsDAG::ActionType::COLUMN)
        {
            const auto * data_type_array = typeid_cast<const DataTypeArray *>(child->result_type.get());
            if (data_type_array == nullptr)
                continue;
            const auto & nested = data_type_array->getNestedType();
            if (!typeid_cast<const DataTypeFloat64 *>(nested.get())
                && !typeid_cast<const DataTypeFloat32 *>(nested.get())
                && !typeid_cast<const DataTypeBFloat16 *>(nested.get()))
                continue;

            Field field = child->column->getField();
            if (field.getType() != Field::Types::Array)
                continue;
            for (const auto & value : field.safeGet<Array>())
            {
                if (value.getType() != Field::Types::Float64)
                    return false;
                reference_vector.push_back(value.safeGet<Float64>());
            }
        }
    }

    if (search_column.empty() || reference_vector.empty())
        return false;

    /// The search column must carry a `Quantize(...)` codec; its parameters describe the codes subcolumn. Resolve it to
    /// the exact storage column: try the full input name first, so a genuine dotted storage column (e.g. a `Nested`
    /// component `n.vec`) is matched as-is rather than conflated with a different top-level column. Only if that fails do
    /// we strip a single leading qualifier (the analyzer qualifies table columns as `table.column`) and retry. Truncating
    /// unconditionally would turn `n.vec` into `vec` and rank the shortlist by the wrong column.
    auto params = findQuantizeCodecParams(*read_step, search_column);
    if (!params && search_column.contains('.'))
    {
        const String unqualified = search_column.substr(search_column.find('.') + 1);
        params = findQuantizeCodecParams(*read_step, unqualified);
        if (params)
            search_column = unqualified;
    }
    if (!params)
        return false;

    /// The rewrite introduces internal columns named `__quantize_*` (the approximate-distance sort key and the constant
    /// function arguments). Blocks permit duplicate column names, so if a column with such a name already flows into the
    /// shortlist the sort key could bind to the user column (or the header build could throw on a type mismatch). Bail
    /// to the exact path on any collision rather than rewrite against an ambiguous header.
    for (const auto & col : expression_node->children.front()->step->getOutputHeader()->getColumnsWithTypeAndName())
        if (col.name.starts_with("__quantize_"))
            return false;

    const String codes_column = search_column + "." + SerializationQuantizedVector::subcolumn_name;
    const String & method = params->method;
    const UInt64 dimensions = params->dimensions;
    const UInt64 bits = params->bits;
    /// The trained `product` method also needs the per-part codebook subcolumn, and ranks with `productQuantizationDistance`.
    const bool is_pq = method == "product";
    const String codebook_column = search_column + "." + SerializationQuantizedVector::product_quantization_subcolumn_name;

    /// ClickHouse lets a physical dotted column shadow a subcolumn (`ColumnsDescription::tryGetColumn` returns the
    /// physical column first). If the table also declares a physical column named like one of these companions - e.g.
    /// `vec Array(Float32) CODEC(Quantize(...))` alongside a physical `vec.quantized` - then resolving the codes/codebook
    /// column by name below would bind that physical column instead of the companion subcolumn and rank the shortlist on
    /// unrelated bytes (silently, when the widths happen to match). Leave the query exact in that case: the same
    /// fall-back-to-exact this rewrite already takes wherever it cannot apply cleanly.
    const auto & storage_columns = read_step->getStorageMetadata()->getColumns();
    if (storage_columns.has(codes_column) || (is_pq && storage_columns.has(codebook_column)))
        return false;

    /// `rabitq`/`turboquant` are cosine-only estimators (they drop the vector norm), so their shortlist ranks by angle.
    /// Do not use it for an L2Distance query: a true L2-nearest row could be dropped before the exact rescore. Leave the
    /// query exact instead. (`int8`, `prefix` and `product` retain enough to rank L2.)
    if (is_l2 && !is_pq && !VectorQuantizer::supportsL2(method))
        return false;

    /// Shortlist size k' = k * fetch_multiplier, clamped so that the inner LimitStep stays eligible for lazy
    /// materialization of the heavy vector column.
    auto context = read_step->getContext();
    const float fetch_multiplier = context->getSettingsRef()[Setting::vector_search_index_fetch_multiplier];
    /// Reject the same invalid values as the vector-similarity-index path (see MergeTreeIndexVectorSimilarity.cpp), so
    /// `vector_search_index_fetch_multiplier` means the same thing on both access paths instead of silently collapsing
    /// (non-finite / non-positive) or inflating (huge / overflowing) the shortlist.
    static constexpr double MAX_FETCH_MULTIPLIER = 1000.0;
    if (!std::isfinite(fetch_multiplier) || fetch_multiplier <= 0.0f
        || static_cast<double>(fetch_multiplier) > MAX_FETCH_MULTIPLIER
        || !std::isfinite(static_cast<double>(fetch_multiplier) * static_cast<double>(n)))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Setting 'vector_search_index_fetch_multiplier' must be greater than 0.0 and less than {}", MAX_FETCH_MULTIPLIER);
    size_t k_prime = n;
    if (fetch_multiplier > 1.0f)
        k_prime = static_cast<size_t>(static_cast<double>(n) * static_cast<double>(fetch_multiplier));
    if (max_limit_for_lazy_materialization != 0)
        k_prime = std::min(k_prime, max_limit_for_lazy_materialization);
    k_prime = std::max(k_prime, n);

    /// All checks passed - rewrite the plan.

    /// 1. Pull the codes subcolumn (and, for the trained `product` method, the per-part codebook subcolumn) into the read
    /// list.
    std::vector<String> extra_columns{codes_column};
    if (is_pq)
        extra_columns.push_back(codebook_column);
    for (const auto & extra_column : extra_columns)
        read_step->addReadColumn(extra_column);

    /// 2. Propagate the extra subcolumns up through the filter/rename chain (bottom-up), so they are available where we
    /// splice the shortlist (above all filters). Each Expression/Filter step would otherwise drop them.
    SharedHeader child_output = read_step->getOutputHeader();
    for (auto * chain_node : chain_nodes | std::views::reverse)
    {
        ActionsDAG * chain_dag = nullptr;
        if (auto * chain_expression = typeid_cast<ExpressionStep *>(chain_node->step.get()))
            chain_dag = &chain_expression->getExpression();
        else if (auto * chain_filter = typeid_cast<FilterStep *>(chain_node->step.get()))
            chain_dag = &chain_filter->getExpression();

        if (chain_dag)
        {
            for (const auto & extra_column : extra_columns)
            {
                if (chain_dag->tryFindInOutputs(extra_column))
                    continue;
                const auto & extra_type = child_output->getByName(extra_column).type;
                const auto & extra_input = chain_dag->addInput(extra_column, extra_type);
                chain_dag->getOutputs().push_back(&extra_input);
            }
        }
        chain_node->step->updateInputHeader(child_output);
        child_output = chain_node->step->getOutputHeader();
    }

    /// `child_output` is now the header just below the splice point (top of the chain, or the read if no chain), with
    /// the codes subcolumn carried through.
    SharedHeader inner_input_header = child_output;
    QueryPlan::Node * inner_child_node = chain_nodes.empty() ? read_node : chain_nodes.front();

    /// 3. Build the approximate-distance expression: `quantizeDistance` for the data-independent methods, or
    ///    `productQuantizationDistance` (with the per-part codebook) for the trained `product` method.
    static constexpr auto approx_column_name = "__quantize_approx_distance";
    ActionsDAG approx_dag(inner_input_header->getColumnsWithTypeAndName());
    const auto & code_node = approx_dag.findInOutputs(codes_column);

    auto reference_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    Array reference_field_array;
    reference_field_array.reserve(reference_vector.size());
    for (Float64 value : reference_vector)
        reference_field_array.emplace_back(value);
    const auto & reference_node = approx_dag.addColumn(
        reference_type->createColumnConst(1, Field(reference_field_array)), reference_type, "__quantize_reference");

    auto string_type = std::make_shared<DataTypeString>();
    const auto & method_node = approx_dag.addColumn(
        string_type->createColumnConst(1, Field(method)), string_type, "__quantize_method");

    auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto & dimensions_node = approx_dag.addColumn(
        uint64_type->createColumnConst(1, Field(dimensions)), uint64_type, "__quantize_dimensions");
    const auto & bits_node = approx_dag.addColumn(
        uint64_type->createColumnConst(1, Field(bits)), uint64_type, "__quantize_bits");

    auto uint8_type = std::make_shared<DataTypeUInt8>();
    const auto & is_l2_node = approx_dag.addColumn(
        uint8_type->createColumnConst(1, Field(static_cast<UInt64>(is_l2 ? 1 : 0))), uint8_type, "__quantize_is_l2");

    const ActionsDAG::Node * approx_node = nullptr;
    if (is_pq)
    {
        /// _approx := __pqDistance(vec.quantized, vec.pq_codebook, ref, dim, m, nbits, is_l2). The codebook is read as a
        /// per-part broadcast column; `bits` carries nbits for the `product` method. `__productQuantizationDistance` is an internal function
        /// built directly (not resolved through FunctionFactory) so it is not exposed as a public SQL function.
        const auto & codebook_node = approx_dag.findInOutputs(codebook_column);
        const auto & m_node = approx_dag.addColumn(
            uint64_type->createColumnConst(1, Field(static_cast<UInt64>(params->m))), uint64_type, "__quantize_m");
        ActionsDAG::NodeRawConstPtrs distance_arguments{
            &code_node, &codebook_node, &reference_node, &dimensions_node, &m_node, &bits_node, &is_l2_node};
        approx_node = &approx_dag.addFunction(createInternalFunctionPQDistanceResolver(), std::move(distance_arguments), approx_column_name);
    }
    else
    {
        /// _approx := __quantizeDistance(vec.quantized, ref, method, dim, bits, is_l2) (internal function, see above).
        ActionsDAG::NodeRawConstPtrs distance_arguments{
            &code_node, &reference_node, &method_node, &dimensions_node, &bits_node, &is_l2_node};
        approx_node = &approx_dag.addFunction(createInternalFunctionQuantizeDistanceResolver(), std::move(distance_arguments), approx_column_name);
    }
    approx_dag.getOutputs().push_back(approx_node);

    /// The codes column (and, for pq, the per-part codebook) are inputs to the distance function. Drop them from the
    /// outputs so they are not carried into - and materialized by - the SortingStep. This is critical for pq, whose
    /// codebook is a large FixedString broadcast as a ColumnConst: the MergeSortingTransform would otherwise expand it
    /// to one full copy per buffered row, exhausting memory on a wide-vector corpus. The nodes stay in the DAG as
    /// consumed inputs (the distance function above still references them).
    /// Exception: keep a companion subcolumn that the rescore expression still consumes - e.g. `SELECT vec.quantized
    /// ... ORDER BY distance LIMIT` selects it - otherwise the rescore above the shortlist would lose its input.
    std::unordered_set<std::string_view> needed_above;
    for (const auto * input : expression.getInputs())
        needed_above.insert(input->result_name);
    std::erase_if(approx_dag.getOutputs(), [&](const ActionsDAG::Node * node)
    {
        const bool is_companion = node->result_name == codes_column || (is_pq && node->result_name == codebook_column);
        return is_companion && !needed_above.contains(node->result_name);
    });

    /// 4. Allocate the inner shortlist nodes (expression -> sorting -> limit), bottom-up so headers chain correctly.
    auto & inner_expression_node = nodes.emplace_back();
    inner_expression_node.step = std::make_unique<ExpressionStep>(inner_input_header, std::move(approx_dag));
    inner_expression_node.step->setStepDescription("quantized shortlist approximate distance");
    inner_expression_node.children = {inner_child_node};

    SortDescription approx_sort_description;
    approx_sort_description.emplace_back(approx_column_name, approx_column_name, /*direction=*/1, /*nulls_direction=*/1);

    auto & inner_sorting_node = nodes.emplace_back();
    inner_sorting_node.step = std::make_unique<SortingStep>(
        inner_expression_node.step->getOutputHeader(), approx_sort_description, k_prime, sorting_step->getSettings());
    inner_sorting_node.step->setStepDescription("quantized shortlist sort");
    inner_sorting_node.children = {&inner_expression_node};

    auto & inner_limit_node = nodes.emplace_back();
    inner_limit_node.step = std::make_unique<LimitStep>(inner_sorting_node.step->getOutputHeader(), k_prime, /*offset=*/0);
    inner_limit_node.step->setStepDescription("quantized shortlist limit");
    inner_limit_node.children = {&inner_sorting_node};

    /// 5. Splice the shortlist above the whole filter/rename chain (between the rescore expression and the chain top).
    /// The rescore expression keeps its output header because it ignores the extra codes/_approx columns. The general
    /// lazy-materialization pass will later defer the heavy vector column on the inner LimitStep (descending through the
    /// chain's Expression/Filter steps), so `vec` is read only for the k' shortlisted rows.
    expression_node->children = {&inner_limit_node};
    expression_node->step->updateInputHeader(inner_limit_node.step->getOutputHeader());

    return true;
}

}
