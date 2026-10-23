#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include "config.h"

#if USE_GPU

#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/GPUAggregatingStep.h>
#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>

#include <Access/EnabledRowPolicies.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUFilterCompiler.h>
#include <GPU/GPUColumns.h>
#include <GPU/GPUTypeMapping.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace
{

#define GPU_COMPRESSED_REFUSE(reason) \
    do \
    { \
        LOG_TRACE(getLogger("GPUCompressedColumns"), "Not aggregating compressed columns on the device: {}", (reason)); \
        return {}; \
    } while (false)

}

namespace Setting
{
    extern const SettingsBool allow_experimental_gpu_aggregation;
    extern const SettingsBool allow_experimental_gpu_filtered_read;
    extern const SettingsFloat gpu_filtered_read_max_selectivity;
    extern const SettingsUInt64 gpu_aggregation_batch_bytes;
    extern const SettingsUInt64 gpu_aggregation_readers;
    extern const SettingsGPUDecompressionMode gpu_aggregation_decompression;
    extern const SettingsBool serialize_query_plan;
    extern const SettingsInt64 max_partitions_to_read;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsOverflowMode timeout_overflow_mode_leaf;
}
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsInt64 max_partitions_to_read;
    extern const MergeTreeSettingsUInt64 max_concurrent_queries;
}
}

namespace DB::QueryPlanOptimizations
{

namespace
{

constexpr size_t max_rows_per_part = (1UL << 31) - 1;

struct ReducedAggregates
{
    Names keys;
    Names arguments;
    std::vector<GPU::GPUAggregationKind> aggregations;
};

std::optional<ReducedAggregates> collectReducedAggregates(const GPUAggregatingStep & aggregating)
{
    const Aggregator::Params & params = aggregating.getParams();

    if (params.aggregates.empty())
        GPU_COMPRESSED_REFUSE("the aggregation has no aggregate functions");

    if (params.only_merge || params.overflow_row || params.max_rows_to_group_by != 0)
        GPU_COMPRESSED_REFUSE("the aggregation merges states, has an overflow row or a group limit");

    auto aggregations = gpuAggregationsOf(params);
    if (!aggregations)
        GPU_COMPRESSED_REFUSE("an aggregate the device cannot reduce by on its own");

    /// A `count` has none.
    Names arguments;
    arguments.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
        if (!aggregate.argument_names.empty())
            arguments.push_back(aggregate.argument_names.front());

    return ReducedAggregates{.keys = params.keys, .arguments = std::move(arguments), .aggregations = std::move(*aggregations)};
}

std::optional<String> passedThroughInputName(const ActionsDAG & dag, const String & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    if (!node)
        return {};

    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();

    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};

    return node->result_name;
}

/// An aggregated argument that an expression between the aggregation and the read computes, and the device computes
/// in its place: of the columns that `input_keys` name in `MatchedChain::read_names`.
struct ComputedArgument
{
    GPU::CompiledGPUFilter expression;
    Names input_keys;
};

struct MatchedChain
{
    QueryPlan::Node * above_read = nullptr;
    ReadFromMergeTree * reading = nullptr;
    std::unordered_map<String, String> read_names;
    std::unordered_map<String, ComputedArgument> computed;
};

std::optional<MatchedChain> matchChain(QueryPlan::Node & aggregating_node, const Names & keys, const Names & arguments)
{
    MatchedChain matched;

    for (const auto & key : keys)
        matched.read_names[key] = key;
    for (const auto & argument : arguments)
        matched.read_names[argument] = argument;

    const NameSet key_names(keys.begin(), keys.end());

    QueryPlan::Node * current = &aggregating_node;

    while (true)
    {
        if (current->children.size() != 1)
            GPU_COMPRESSED_REFUSE("a step with other than one input below the aggregation");

        QueryPlan::Node * child = current->children.front();
        IQueryPlanStep * step = child->step.get();

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            matched.above_read = current;
            matched.reading = reading;
            return matched;
        }

        if (typeid_cast<FilterStep *>(step))
            GPU_COMPRESSED_REFUSE("a filter between the aggregation and the read");

        const auto * expression = typeid_cast<ExpressionStep *>(step);
        if (!expression)
            GPU_COMPRESSED_REFUSE("a step that is neither an expression nor the read of a MergeTree table");

        if (!expression->getTransformTraits().preserves_number_of_rows)
            GPU_COMPRESSED_REFUSE("an expression that does not preserve the number of rows");

        const ActionsDAG & dag = expression->getExpression();

        std::vector<std::pair<String, String>> inputs_of_computed;
        for (auto it = matched.read_names.begin(); it != matched.read_names.end();)
        {
            auto & [traced, name] = *it;
            if (const auto input_name = passedThroughInputName(dag, name))
            {
                name = *input_name;
                ++it;
                continue;
            }

            /// An aggregated argument the device can compute, of columns this step passes through from below.
            const bool is_argument = !key_names.contains(traced) && !matched.computed.contains(traced) && traced.find('\n') == String::npos;
            if (!is_argument)
                GPU_COMPRESSED_REFUSE("an expression that computes a key or the input of a computed argument instead of passing it through");

            String refusal;
            auto compiled = GPU::compileGPUExpression(dag, name, refusal);
            if (!compiled)
                GPU_COMPRESSED_REFUSE("an expression that computes an aggregated column the device does not: " + refusal);

            ComputedArgument computed{.expression = std::move(*compiled), .input_keys = {}};
            for (size_t i = 0; i < computed.expression.columns.size(); ++i)
            {
                computed.input_keys.push_back(traced + "\n" + std::to_string(i));
                inputs_of_computed.emplace_back(computed.input_keys.back(), computed.expression.columns[i].name);
            }
            matched.computed.emplace(traced, std::move(computed));
            it = matched.read_names.erase(it);
        }

        /// From the inputs of this step, so the next one down maps them further.
        for (auto & [traced, name] : inputs_of_computed)
            matched.read_names.emplace(std::move(traced), std::move(name));

        current = child;
    }
}

/// Whether the device can read a column as the compressed blocks of a codec it expands: those of the part's default codec,
/// or a codec of the column's own that is one such codec alone, or `BitPacking` and then one, which a pipeline such as
/// `CODEC(Delta, ZSTD)` is not.
bool deviceExpandsColumnCodec(const ColumnsDescription & columns, const String & name)
{
    if (!columns.hasCompressionCodec(name))
        return true;
    if (columns.hasExplicitDefaultCompressionCodec(name))
        return false;
    const ColumnDescription & column = columns.get(name);
    const CompressionCodecPtr codec = CompressionCodecFactory::instance().get(column.codec, column.type);
    return GPU::deviceCodecOf(*codec).has_value();
}


struct ReadColumns
{
    std::vector<NameAndTypePair> keys;
    /// One per column the aggregates are of, once each. Without keys, every aggregate of a column must be by one
    /// function, as the column is reduced into itself.
    std::vector<ReadFromGPUCompressedColumns::ColumnToReduce> columns;
    /// With keys only.
    std::vector<ReadFromGPUCompressedColumns::DerivedColumn> derived;
    std::vector<ReadFromGPUCompressedColumns::AggregateToCompute> aggregates;
};

std::optional<ReadColumns> matchReducedColumns(
    const GPUAggregatingStep & aggregating,
    const ReadFromMergeTree & reading,
    const std::unordered_map<String, String> & read_names,
    const std::unordered_map<String, ComputedArgument> & computed,
    const Names & keys,
    const std::vector<GPU::GPUAggregationKind> & aggregations)
{
    const Block & read_header = *reading.getOutputHeader();
    const StorageMetadataPtr metadata = reading.getStorageMetadata();
    const ColumnsDescription & table_columns = metadata->getColumns();

    const auto & aggregates = aggregating.getParams().aggregates;

    std::unordered_map<String, GPU::GPUAggregationKind> aggregation_by_read_name;
    std::unordered_map<String, size_t> key_position_by_read_name;
    ReadColumns matched;

    for (const auto & key : keys)
    {
        const auto read_name = read_names.find(key);
        if (read_name == read_names.end())
            GPU_COMPRESSED_REFUSE("a key column the descent did not translate");

        const String & name = read_name->second;

        const ColumnWithTypeAndName * read_column = read_header.findByName(name);
        if (!read_column)
            GPU_COMPRESSED_REFUSE("a key column that is not in the read step's header");

        if (metadata->virtuals.has(name))
            GPU_COMPRESSED_REFUSE("a key column that is a virtual column");

        if (!table_columns.hasPhysical(name))
            GPU_COMPRESSED_REFUSE("a key column that is not a stored column of the table");

        if (!table_columns.getPhysical(name).type->equals(*read_column->type))
            GPU_COMPRESSED_REFUSE("a key column whose type differs from the table's");

        if (!deviceExpandsColumnCodec(table_columns, name))
            GPU_COMPRESSED_REFUSE("a key column with a CODEC of its own the device does not expand");

        const auto [seen, inserted] = key_position_by_read_name.emplace(name, matched.keys.size());
        if (!inserted)
            GPU_COMPRESSED_REFUSE("a column that is a key twice over");

        matched.keys.emplace_back(name, read_column->type);
    }

    const bool keyed = !keys.empty();

    /// With keys, the position among `matched.columns` of each aggregated column, by its name in the read.
    std::unordered_map<String, size_t> column_position_by_read_name;

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & aggregate = aggregates[i];
        const GPU::GPUAggregationKind aggregation = aggregations[i];
        const DataTypePtr result_type = aggregate.function->getResultType();

        if (aggregate.argument_names.empty())
        {
            if (!keyed || !GPU::canGroupByOnDevice(nullptr, *result_type, aggregation))
                GPU_COMPRESSED_REFUSE("an aggregate of no column the device does not compute");
            matched.aggregates.push_back({
                .aggregation = aggregation,
                .column = std::nullopt,
                .derived = std::nullopt,
                .result_type = result_type,
                .description = aggregate.column_name,
            });
            continue;
        }

        if (const auto computed_argument = computed.find(aggregate.argument_names.front()); computed_argument != computed.end())
        {
            static const DataTypePtr float64 = std::make_shared<DataTypeFloat64>();
            if (!keyed || !GPU::canGroupByOnDevice(float64.get(), *result_type, aggregation))
                GPU_COMPRESSED_REFUSE("an aggregate of a computed argument the device does not group by");

            ReadFromGPUCompressedColumns::DerivedColumn derived{
                .program = computed_argument->second.expression.program,
                .columns = {},
                .description = aggregate.argument_names.front(),
            };
            for (const auto & input_key : computed_argument->second.input_keys)
            {
                const auto input_name = read_names.find(input_key);
                if (input_name == read_names.end())
                    GPU_COMPRESSED_REFUSE("a column of a computed argument the descent did not translate");
                const String & name = input_name->second;

                const ColumnWithTypeAndName * read_column = read_header.findByName(name);
                if (!read_column || metadata->virtuals.has(name) || !table_columns.hasPhysical(name))
                    GPU_COMPRESSED_REFUSE("a column of a computed argument that is not a stored column the read produces");
                if (!table_columns.getPhysical(name).type->equals(*read_column->type) || !read_column->type->equals(*float64))
                    GPU_COMPRESSED_REFUSE("a column of a computed argument that is not a stored `Float64`");
                if (!deviceExpandsColumnCodec(table_columns, name))
                    GPU_COMPRESSED_REFUSE("a column of a computed argument with a CODEC of its own the device does not expand");
                if (key_position_by_read_name.contains(name))
                    GPU_COMPRESSED_REFUSE("a column that is both a key and the input of a computed argument");

                const auto [position, inserted] = column_position_by_read_name.emplace(name, matched.columns.size());
                if (inserted)
                    matched.columns.push_back({
                        .column = NameAndTypePair(name, read_column->type),
                        .result_type = read_column->type,
                        .aggregation = aggregation,
                    });
                derived.columns.push_back(position->second);
            }

            matched.aggregates.push_back({
                .aggregation = aggregation,
                .column = std::nullopt,
                .derived = matched.derived.size(),
                .result_type = result_type,
                .description = aggregate.column_name,
            });
            matched.derived.push_back(std::move(derived));
            continue;
        }

        const auto read_name = read_names.find(aggregate.argument_names.front());
        if (read_name == read_names.end())
            GPU_COMPRESSED_REFUSE("an aggregated column the descent did not translate");

        const String & name = read_name->second;

        const ColumnWithTypeAndName * read_column = read_header.findByName(name);
        if (!read_column)
            GPU_COMPRESSED_REFUSE("an aggregated column that is not in the read step's header");

        if (metadata->virtuals.has(name))
            GPU_COMPRESSED_REFUSE("an aggregated column that is a virtual column");

        if (!table_columns.hasPhysical(name))
            GPU_COMPRESSED_REFUSE("an aggregated column that is not a stored column of the table");

        const DataTypePtr & type = read_column->type;
        if (!table_columns.getPhysical(name).type->equals(*type))
            GPU_COMPRESSED_REFUSE("an aggregated column whose type differs from the table's");

        if (!deviceExpandsColumnCodec(table_columns, name))
            GPU_COMPRESSED_REFUSE("an aggregated column with a CODEC of its own the device does not expand");

        if (key_position_by_read_name.contains(name))
            GPU_COMPRESSED_REFUSE("a column that is both a key and an aggregated column");

        if (keyed)
        {
            /// The device reads and sends a column once, however many aggregates are of it.
            if (!GPU::canGroupByOnDevice(type.get(), *result_type, aggregation))
                GPU_COMPRESSED_REFUSE("an aggregate the device does not group by");

            const auto [position, inserted] = column_position_by_read_name.emplace(name, matched.columns.size());
            if (inserted)
                matched.columns.push_back({
                    .column = NameAndTypePair(name, type),
                    .result_type = type,
                    .aggregation = aggregation,
                });
            matched.aggregates.push_back({
                .aggregation = aggregation,
                .column = position->second,
                .derived = std::nullopt,
                .result_type = result_type,
                .description = aggregate.column_name,
            });
            continue;
        }

        /// Without keys, the read emits one row per part in its own header, each column reduced into itself.
        if (!result_type->equals(*type))
            GPU_COMPRESSED_REFUSE("an aggregate whose result type differs from its argument's");

        if (!GPU::canReduceOnDevice(*type, *type, aggregation))
            GPU_COMPRESSED_REFUSE("an aggregated column of a type the device does not reduce into itself");

        const auto [seen, inserted] = aggregation_by_read_name.emplace(name, aggregation);
        if (!inserted && seen->second != aggregation)
            GPU_COMPRESSED_REFUSE("a column that two aggregates reduce by different aggregate functions");
    }

    for (const auto & read_column : read_header)
    {
        if (key_position_by_read_name.contains(read_column.name))
        {
            if (!GPU::isStringKey(*read_column.type) && !GPU::groupKeyElementTypeOf(*read_column.type))
                GPU_COMPRESSED_REFUSE("a key column of a type the device has no element type for");
            continue;
        }

        if (!GPU::columnTypeOf(*read_column.type))
            GPU_COMPRESSED_REFUSE("a column of a type the device has no element type for");

        if (keyed)
        {
            if (!column_position_by_read_name.contains(read_column.name))
                GPU_COMPRESSED_REFUSE("the read produces a column nothing aggregates or groups by");
            continue;
        }

        const auto aggregation = aggregation_by_read_name.find(read_column.name);
        if (aggregation == aggregation_by_read_name.end())
            GPU_COMPRESSED_REFUSE("the read produces a column nothing aggregates or groups by");

        matched.columns.push_back({
            .column = NameAndTypePair(read_column.name, read_column.type),
            .result_type = read_column.type,
            .aggregation = aggregation->second,
        });
    }

    if (keyed)
    {
        DataTypes key_types;
        DataTypes argument_types;
        DataTypes result_types;
        std::vector<GPU::GPUAggregationKind> aggregate_kinds;
        for (const auto & key : matched.keys)
            key_types.push_back(key.type);
        for (const auto & aggregate : matched.aggregates)
        {
            static const DataTypePtr float64 = std::make_shared<DataTypeFloat64>();
            argument_types.push_back(
                aggregate.column ? matched.columns[*aggregate.column].column.type : (aggregate.derived ? float64 : nullptr));
            result_types.push_back(aggregate.result_type);
            aggregate_kinds.push_back(aggregate.aggregation);
        }

        if (matched.columns.empty())
            GPU_COMPRESSED_REFUSE("a GROUP BY that aggregates no column");

        if (!GPU::canGroupByReduceOnDevice(key_types, argument_types, result_types, aggregate_kinds))
            GPU_COMPRESSED_REFUSE("keys or aggregated columns of types the device does not group by");
    }

    return matched;
}

/// The `PREWHERE` of a keyed read as a program for the device, when the read has one and the
/// device can evaluate it. The read's header is without the predicate's column, so the columns the
/// predicate reads come along as a group of their own; `readIsOfWholeParts` still holds, since a
/// `PREWHERE` filters rows, not parts.
struct MatchedFilter
{
    std::optional<ReadFromGPUCompressedColumns::DeviceFilter> filter;
};

std::optional<MatchedFilter> matchPrewhere(const ReadFromMergeTree & reading, bool keyed)
{
    const PrewhereInfoPtr prewhere = reading.getPrewhereInfo();
    if (!prewhere)
        return MatchedFilter{};

    if (!keyed)
        GPU_COMPRESSED_REFUSE("a PREWHERE in a keyless read, whose one row per part would have to be filtered on the CPU");

    if (!prewhere->need_filter)
        GPU_COMPRESSED_REFUSE("a PREWHERE that does not filter");

    if (!prewhere->remove_prewhere_column)
        GPU_COMPRESSED_REFUSE("a PREWHERE whose column the read keeps");

    /// The read's other outputs are the `PREWHERE`'s outputs, which must be its inputs as they
    /// are for the read's header to name stored columns.
    for (const ActionsDAG::Node * output : prewhere->prewhere_actions.getOutputs())
    {
        if (output->result_name == prewhere->prewhere_column_name)
            continue;
        if (output->type != ActionsDAG::ActionType::INPUT)
            GPU_COMPRESSED_REFUSE("a PREWHERE that computes a column besides its predicate");
    }

    String refusal;
    auto compiled = GPU::compileGPUFilter(prewhere->prewhere_actions, prewhere->prewhere_column_name, refusal);
    if (!compiled)
        GPU_COMPRESSED_REFUSE("a PREWHERE the device does not evaluate: " + refusal);

    const StorageMetadataPtr metadata = reading.getStorageMetadata();
    const ColumnsDescription & table_columns = metadata->getColumns();

    for (const auto & column : compiled->columns)
    {
        if (metadata->virtuals.has(column.name))
            GPU_COMPRESSED_REFUSE("a PREWHERE over a virtual column");

        if (!table_columns.hasPhysical(column.name))
            GPU_COMPRESSED_REFUSE("a PREWHERE over a column that is not a stored column of the table");

        if (!table_columns.getPhysical(column.name).type->equals(*column.type))
            GPU_COMPRESSED_REFUSE("a PREWHERE over a column whose type differs from the table's");

        if (!deviceExpandsColumnCodec(table_columns, column.name))
            GPU_COMPRESSED_REFUSE("a PREWHERE over a column with a CODEC of its own the device does not expand");
    }

    return MatchedFilter{ReadFromGPUCompressedColumns::DeviceFilter{
        .program = compiled->program,
        .columns = std::move(compiled->columns),
        .description = prewhere->prewhere_column_name,
    }};
}

bool readIsOfWholeParts(const ReadFromMergeTree & reading)
{
    /// A row-level filter drops rows, and a filtered part's result is not the result over its
    /// whole column. A filter pushed into the read as `filter_actions_dag` only prunes what is
    /// read, which `matchParts` sees; a `PREWHERE` is `matchPrewhere`'s to take or refuse.
    /// Each of the last two has a deferred form that a lazy `FINAL` plan leaves behind.
    if (reading.getRowLevelFilter())
        GPU_COMPRESSED_REFUSE("a row-level filter in the read");

    if (reading.getDeferredPrewhereInfo() || reading.getDeferredRowLevelFilter())
        GPU_COMPRESSED_REFUSE("a deferred PREWHERE or row-level filter in the read");

    /// `FINAL` collapses rows across parts, so a per-part result is not a partial result of anything.
    /// Sampling reads a fraction of each part and scales the result.
    if (reading.isQueryWithFinal() || reading.isQueryWithSampling())
        GPU_COMPRESSED_REFUSE("the read is FINAL or sampled");

    /// Reads that produce something other than the table's stored columns: a direct read from a
    /// text index adds virtual columns, a vector search replaces a column with `_distance` or
    /// filters rows by a candidate list, and a top-k read filters by a threshold.
    if (!reading.getIndexReadTasks().empty() || reading.getVectorSearchParameters() || reading.isVectorColumnReplaced())
        GPU_COMPRESSED_REFUSE("the read is from an index or a vector search");

    if (reading.isSelectedForTopKFilterOptimization())
        GPU_COMPRESSED_REFUSE("the read is filtered by a top-k threshold");

    if (!reading.getProjectionIndexReadDescription().read_ranges.empty())
        GPU_COMPRESSED_REFUSE("the read uses a projection index");

    /// A read with an output port per partition feeds several aggregations rather than one.
    if (reading.willOutputEachPartitionThroughSeparatePort())
        GPU_COMPRESSED_REFUSE("the read outputs each partition through its own port");

    /// Every replica would reduce every part it is given, and the coordination that keeps them from
    /// reading the same rows twice is in the reader this replaces.
    if (reading.isParallelReadingFromReplicas() || reading.isParallelReadingEnabled() || reading.getDistributedReadBucketCount() > 0)
        GPU_COMPRESSED_REFUSE("the read is distributed across replicas");

    /// A `UNIQUE KEY` table filters rows by a delete bitmap while reading, which the sequential
    /// source that fills the cache does not apply.
    if (reading.getStorageMetadata()->hasUniqueKey())
        GPU_COMPRESSED_REFUSE("the table has a UNIQUE KEY");

    return true;
}

/// Whether the query around the read is one whose answer does not depend on how much was read -
/// because on a cache hit nothing is.
bool queryLimitsAllowReading(const ReadFromMergeTree & reading)
{
    const ContextPtr context = reading.getContext();
    const Settings & settings = context->getSettingsRef();

    /// `ReadFromGPUCompressedColumns` has no serialization, so it must not end up in a plan that is
    /// sent somewhere.
    if (settings[Setting::serialize_query_plan])
        GPU_COMPRESSED_REFUSE("`serialize_query_plan` is on");

    /// A transaction can see Outdated parts, and what it may see is decided by the reader.
    if (context->getCurrentTransaction())
        GPU_COMPRESSED_REFUSE("the query is in a transaction");

    /// These bound how much a query may read, and on a cache hit it reads nothing - so the same
    /// query would throw on the run that fills the cache and succeed on the next one. Refusing the
    /// optimization keeps the limit meaning what it says.
    if (settings[Setting::max_rows_to_read] != 0 || settings[Setting::max_bytes_to_read] != 0
        || settings[Setting::max_rows_to_read_leaf] != 0 || settings[Setting::max_bytes_to_read_leaf] != 0)
        GPU_COMPRESSED_REFUSE("a limit on how much may be read is set");

    /// With `break`, a query that runs out of time stops reading and returns what it has. A part
    /// whose read stopped half way has no partial answer here - the row the source emits claims to
    /// be the result over that part's every row - so this path only takes queries whose time limit
    /// throws.
    if (settings[Setting::timeout_overflow_mode] != OverflowMode::THROW
        || settings[Setting::timeout_overflow_mode_leaf] != OverflowMode::THROW)
        GPU_COMPRESSED_REFUSE("`timeout_overflow_mode` is not `throw`");

    /// The rewrite bypasses the reader's `checkLimits`, so mirror the two things it establishes:
    /// `max_concurrent_queries`, which needs a holder kept for the length of the execution that
    /// this pass has nowhere to put, and `max_partitions_to_read`, which the reader would have
    /// thrown `TOO_MANY_PARTITIONS` for.
    const auto & data_settings = *reading.getMergeTreeData().getSettings();

    if (data_settings[MergeTreeSetting::max_concurrent_queries] > 0)
        GPU_COMPRESSED_REFUSE("the table has `max_concurrent_queries` set");

    const Int64 max_partitions_to_read = settings[Setting::max_partitions_to_read].changed
        ? settings[Setting::max_partitions_to_read]
        : data_settings[MergeTreeSetting::max_partitions_to_read];

    if (max_partitions_to_read > 0)
    {
        std::set<String> partitions;
        for (const auto & part_with_ranges : reading.getParts())
        {
            partitions.insert(part_with_ranges.data_part->info.getPartitionId());
            if (partitions.size() > static_cast<size_t>(max_partitions_to_read))
                GPU_COMPRESSED_REFUSE("the read is over more than `max_partitions_to_read` partitions");
        }
    }

    /// A row policy filters rows that a part's whole result ignores. Without a database name the
    /// policy cannot be looked up, so that case is refused rather than assumed to be empty.
    const StorageID storage_id = reading.getStorageID();
    if (!storage_id.hasDatabase())
        GPU_COMPRESSED_REFUSE("the table has no database name to look a row policy up by");

    if (const auto row_policy_filter = context->getRowPolicyFilter(
            storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        row_policy_filter && !row_policy_filter->isAlwaysTrue())
        GPU_COMPRESSED_REFUSE("a row policy applies to the table");

    return true;
}

/// The parts to read, each whole or in the ranges of marks the indexes left of it: in ranges only for a keyed
/// aggregation without columns of varying width, which a part's every row needs the same from.
std::optional<std::vector<ReadFromGPUCompressedColumns::PartToRead>> matchParts(
    const ReadFromMergeTree & reading, const RangesInDataParts & parts_with_ranges, const ReadColumns & columns)
{
    const auto is_variable = [](const NameAndTypePair & column) { return GPU::isStringKey(*column.type); };
    const bool fixed_width = std::ranges::none_of(columns.keys, is_variable)
        && std::ranges::none_of(columns.columns, [&](const auto & column) { return is_variable(column.column); });
    const bool ranges_allowed = !columns.keys.empty() && fixed_width;

    const MergeTreeData::MutationsSnapshotPtr & mutations = reading.getMutationsSnapshot();

    if (!mutations)
        GPU_COMPRESSED_REFUSE("the read has no mutations snapshot");

    if (mutations->hasDataMutations() || mutations->hasAlterMutations() || mutations->hasMetadataMutations()
        || mutations->hasPatchParts() || mutations->hasLightweightDeletedMask())
        GPU_COMPRESSED_REFUSE("the table has mutations, patch parts or a lightweight delete mask");

    if (reading.getMergeTreeData().hasEnabledMaskingPolicies(reading.getContext()))
        GPU_COMPRESSED_REFUSE("a masking policy applies to the table");

    std::vector<ReadFromGPUCompressedColumns::PartToRead> parts;
    parts.reserve(parts_with_ranges.size());

    for (const auto & part_with_ranges : parts_with_ranges)
    {
        const DataPartPtr & part = part_with_ranges.data_part;
        if (!part)
            GPU_COMPRESSED_REFUSE("a part without a data part object");

        if (part->isProjectionPart() || part_with_ranges.parent_part)
            GPU_COMPRESSED_REFUSE("a projection part");

        const bool whole = part_with_ranges.getRowsCount() == part->rows_count;
        if (!whole && !ranges_allowed)
            GPU_COMPRESSED_REFUSE("a part of which less than every row is read, of an aggregation without keys or of strings");

        if (part->hasLightweightDelete())
            GPU_COMPRESSED_REFUSE("a part with a lightweight delete mask");

        if (!mutations->getOnFlyMutationCommandsForPart(part).empty() || !mutations->getPatchesForPart(part).empty())
            GPU_COMPRESSED_REFUSE("a part with an on-the-fly mutation or a patch");

        if (part->rows_count > max_rows_per_part)
            GPU_COMPRESSED_REFUSE("a part with more rows than the device can view at once");

        if (part->rows_count == 0)
            continue;

        if (part->getType() != MergeTreeDataPartType::Wide)
            GPU_COMPRESSED_REFUSE("a part that is not wide");

        if (!part->default_codec || !GPU::codecOf(part->default_codec->getMethodByte()))
            GPU_COMPRESSED_REFUSE("a part written with a codec the device cannot expand");

        /// A column's `.bin` file holds every value of the part only under the default
        /// serialization: a sparse one leaves the default values out and keeps their positions in
        /// a stream of its own.
        const auto part_stores = [&](const NameAndTypePair & column)
        {
            const auto part_column = part->tryGetColumn(column.name);
            if (!part_column || !part_column->type->equals(*column.type))
                return false;
            if (!part->hasColumnFiles(*part_column))
                return false;
            return part->getSerialization(column.name)->getKindStack() == ISerialization::KindStack{ISerialization::Kind::DEFAULT};
        };

        /// The device reads a string's bytes from its own file and their sizes from the `.size` stream
        /// beside it, which parts written before `string_serialization_version` was `with_size_stream`
        /// do not have: their sizes are inline, a varint before each string.
        const auto part_stores_sizes = [&](const NameAndTypePair & column)
        {
            if (!GPU::isStringKey(*column.type))
                return true;
            ISerialization::SubstreamPath sizes_path;
            sizes_path.push_back(ISerialization::Substream::StringSizes);
            return IMergeTreeDataPart::getStreamNameForColumn(column, sizes_path, ".bin", part->checksums, part->storage.getSettings()).has_value();
        };

        for (const auto & key : columns.keys)
        {
            if (!part_stores(key))
                GPU_COMPRESSED_REFUSE("a part that does not store a key column with the table's type and the default serialization");
            if (!part_stores_sizes(key))
                GPU_COMPRESSED_REFUSE("a part that keeps the sizes of a string key inline rather than in a stream of their own");
        }

        for (const auto & column : columns.columns)
        {
            if (!part_stores(column.column))
                GPU_COMPRESSED_REFUSE("a part that does not store an aggregated column with the table's type and the default serialization");
            if (!part_stores_sizes(column.column))
                GPU_COMPRESSED_REFUSE("a part that keeps the sizes of an aggregated string inline rather than in a stream of their own");
        }

        parts.push_back({.part = part, .ranges = whole ? MarkRanges{} : part_with_ranges.ranges});
    }

    return parts;
}

/// A `HAVING` right above a `GROUP BY` the device computes, which the device evaluates over the groups before it sends
/// them back. The filter step stays and evaluates it again over the groups that are left.
void pushGroupFilterToDevice(const FilterStep & filter, ReadFromGPUCompressedColumns & source)
{
    String refusal;
    auto compiled = GPU::compileGPUFilter(filter.getExpression(), filter.getFilterColumnName(), refusal);
    if (!compiled)
    {
        LOG_TRACE(getLogger("GPUCompressedColumns"), "Not filtering the groups on the device: {}", refusal);
        return;
    }

    const Block & header = *source.getOutputHeader();
    ReadFromGPUCompressedColumns::GroupFilter group_filter{.program = compiled->program, .columns = {}, .description = filter.getFilterColumnName()};
    for (const auto & column : compiled->columns)
    {
        if (!header.has(column.name))
        {
            LOG_TRACE(getLogger("GPUCompressedColumns"), "Not filtering the groups on the device: a column the groups do not have");
            return;
        }
        const auto & group_column = header.getByName(column.name);
        if (!group_column.type->equals(*column.type) || GPU::isStringKey(*group_column.type) || !GPU::filterElementTypeOf(*group_column.type))
        {
            LOG_TRACE(getLogger("GPUCompressedColumns"), "Not filtering the groups on the device: a column of a type the device does not compare");
            return;
        }
        group_filter.columns.push_back(static_cast<uint32_t>(header.getPositionByName(column.name)));
    }

    source.setGroupFilter(std::move(group_filter));
}

}

namespace
{

/// An aggregation that a rewrite made after the planner, such as the decorrelation of a subquery, is an `AggregatingStep`
/// where the planner would have made a `GPUAggregatingStep`: it is made one here, where the device can run it, so that
/// the device runs it as it would have, over the compressed columns where it can.
void replaceWithGPUAggregating(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings)
{
    const auto * aggregating = typeid_cast<const AggregatingStep *>(node.step.get());
    if (!aggregating || node.children.size() != 1 || !settings.gpu_aggregation)
        return;

    /// What the planner refuses the device as well: the step must produce final values of one aggregation.
    if (!aggregating->isFinal() || aggregating->isGroupingSets() || aggregating->isGroupByUseNulls() || aggregating->isMergingSkipped()
        || !aggregating->getGroupBySortDescription().empty() || aggregating->explicitSortingRequired())
        return;

    const SharedHeader input_header = aggregating->getInputHeaders().front();
    if (!GPUAggregatingStep::canRunOnDevice(*input_header, aggregating->getParams()))
        return;

    auto on_device = std::make_unique<GPUAggregatingStep>(input_header, aggregating->getParams(), settings.gpu_aggregation_batch_bytes);
    if (!blocksHaveEqualStructure(*on_device->getOutputHeader(), *aggregating->getOutputHeader()))
        return;

    LOG_DEBUG(getLogger("GPUCompressedColumns"), "Aggregating on the device an aggregation made after the planner: {}", aggregating->getStepDescription());
    on_device->setStepDescription(*aggregating);
    node.step = std::move(on_device);
}

}

bool optimizeAggregationFromGPUCompressedColumns(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    replaceWithGPUAggregating(node, settings);

    /// The plan is optimized from the top down, so the aggregation below a `HAVING` is made into a read of its own here.
    if (const auto * filter = typeid_cast<const FilterStep *>(node.step.get()); filter && node.children.size() == 1)
    {
        QueryPlan::Node & child = *node.children.front();
        replaceWithGPUAggregating(child, settings);
        if (!typeid_cast<const GPUAggregatingStep *>(child.step.get()) || !optimizeAggregationFromGPUCompressedColumns(child, nodes, settings))
            return false;
        if (auto * source = typeid_cast<ReadFromGPUCompressedColumns *>(child.step.get()); source && source->isGrouped())
            pushGroupFilterToDevice(*filter, *source);
        return true;
    }

    if (settings.make_distributed_plan || settings.enable_parallel_replicas)
        GPU_COMPRESSED_REFUSE("the plan is being made distributed");

    if (settings.force_use_projection || !settings.force_projection_name.empty())
        GPU_COMPRESSED_REFUSE("the query insists on using a projection");

    const auto * aggregating = typeid_cast<const GPUAggregatingStep *>(node.step.get());
    if (!aggregating)
        return false;

    const auto reduced = collectReducedAggregates(*aggregating);
    if (!reduced)
        return false;

    auto chain = matchChain(node, reduced->keys, reduced->arguments);
    if (!chain)
        return false;

    ReadFromMergeTree * reading = chain->reading;
    const ContextPtr context = reading->getContext();

    if (!context->getSettingsRef()[Setting::allow_experimental_gpu_aggregation])
        GPU_COMPRESSED_REFUSE("`allow_experimental_gpu_aggregation` is off");

    if (!readIsOfWholeParts(*reading))
        return false;

    auto columns = matchReducedColumns(*aggregating, *reading, chain->read_names, chain->computed, reduced->keys, reduced->aggregations);
    if (!columns)
        return false;

    auto filter = matchPrewhere(*reading, !columns->keys.empty());
    if (!filter)
        return false;

    if (!reading->getAnalyzedResult())
        reading->setAnalyzedResult(reading->selectRangesToRead());

    /// The device reads the ranges itself, where the skip indexes that the read applies to the granules it reads are not.
    const auto analysis = reading->selectRangesToReadApplyingSkipIndexes();
    if (!analysis)
        GPU_COMPRESSED_REFUSE("the read has no analyzed result");

    if (!analysis->isUsable())
        GPU_COMPRESSED_REFUSE("the read exceeded its row limits");

    if (!queryLimitsAllowReading(*reading))
        return false;

    if (analysis->readFromProjection())
        GPU_COMPRESSED_REFUSE("the read is from a projection");

    if (analysis->sampling.use_sampling || analysis->sampling.read_nothing)
        GPU_COMPRESSED_REFUSE("the read is sampled");

    auto parts = matchParts(*reading, analysis->parts_with_ranges, *columns);
    if (!parts)
        return false;

    LOG_DEBUG(
        getLogger("GPUCompressedColumns"),
        "Aggregating compressed columns on the device: {} parts, {} of them in ranges of marks, {} keys, {} columns{}",
        parts->size(),
        std::ranges::count_if(*parts, [](const auto & part) { return !part.ranges.empty(); }),
        columns->keys.size(),
        columns->columns.size(),
        filter->filter ? ", a PREWHERE over " + std::to_string(filter->filter->columns.size()) + " columns" : "");

    const bool keyed = !columns->keys.empty();

    /// With keys, the read takes the place of the aggregation and emits its header; without, it emits its own and the
    /// aggregation above folds a row per part.
    SharedHeader header = keyed ? aggregating->getOutputHeader() : reading->getOutputHeader();

    if (!keyed)
    {
        const SharedHeaders & headers_above_read = chain->above_read->step->getInputHeaders();
        if (headers_above_read.size() != 1 || !blocksHaveEqualStructure(*headers_above_read.front(), *header))
            GPU_COMPRESSED_REFUSE("the step above the read would see a different header");
    }

    auto & source_node = nodes.emplace_back();

    source_node.step = std::make_unique<ReadFromGPUCompressedColumns>(
        header,
        std::move(columns->keys),
        std::move(columns->columns),
        std::move(columns->derived),
        std::move(columns->aggregates),
        std::move(filter->filter),
        std::move(*parts),
        reading->getStorageSnapshot(),
        context,
        context->getSettingsRef()[Setting::gpu_aggregation_batch_bytes],
        reading->getNumStreams(),
        context->getSettingsRef()[Setting::gpu_aggregation_readers],
        context->getSettingsRef()[Setting::gpu_aggregation_decompression]);

    source_node.step->setStepDescription(
        keyed ? "Compressed columns decompressed and grouped on the device, one row per group"
              : "Compressed columns decompressed and reduced on the device, one row per part",
        settings.max_step_description_length);

    if (keyed)
    {
        /// The read groups every part on the device and emits the aggregation's result itself.
        node.step = std::move(source_node.step);
        node.children.clear();
        nodes.pop_back();
        return true;
    }

    chain->above_read->children.front() = &source_node;
    return true;
}

namespace
{

#define GPU_FILTERED_READ_REFUSE(reason) \
    do \
    { \
        LOG_TRACE(getLogger("GPUCompressedColumns"), "Not filtering the read on the device: {}", (reason)); \
        return {}; \
    } while (false)

/// A stored column of the table, of the table's type, that the device reads as the compressed blocks of a codec it
/// expands.
bool isDeviceReadableColumn(const ReadFromMergeTree & reading, const NameAndTypePair & column)
{
    const StorageMetadataPtr metadata = reading.getStorageMetadata();
    const ColumnsDescription & table_columns = metadata->getColumns();
    return !metadata->virtuals.has(column.name) && table_columns.hasPhysical(column.name)
        && table_columns.getPhysical(column.name).type->equals(*column.type) && deviceExpandsColumnCodec(table_columns, column.name);
}

const ActionsDAG::Node * withoutAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

/// The conjuncts of a predicate, through `and`s and aliases.
void collectConjuncts(const ActionsDAG::Node * node, ActionsDAG::NodeRawConstPtrs & conjuncts)
{
    node = withoutAliases(node);
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "and")
    {
        for (const auto * child : node->children)
            collectConjuncts(child, conjuncts);
        return;
    }
    conjuncts.push_back(node);
}

/// A `PREWHERE` split into what the device evaluates and what the CPU does over the rows the device keeps.
struct SplitPrewhere
{
    ReadFromGPUCompressedColumns::DeviceFilter device;
    /// The conjuncts the device evaluates: for the rows it sends back, each is true.
    std::unordered_set<const ActionsDAG::Node *> on_device;
    ActionsDAG::NodeRawConstPtrs on_host;
};

std::optional<SplitPrewhere> splitPrewhere(const ReadFromMergeTree & reading)
{
    const PrewhereInfoPtr prewhere = reading.getPrewhereInfo();
    if (!prewhere)
        GPU_FILTERED_READ_REFUSE("the read has no PREWHERE");
    if (!prewhere->need_filter || !prewhere->remove_prewhere_column)
        GPU_FILTERED_READ_REFUSE("a PREWHERE that does not filter, or whose column the read keeps");

    const ActionsDAG & dag = prewhere->prewhere_actions;
    const ActionsDAG::Node * predicate = dag.tryFindInOutputs(prewhere->prewhere_column_name);
    if (!predicate)
        GPU_FILTERED_READ_REFUSE("a PREWHERE without its predicate among its outputs");

    ActionsDAG::NodeRawConstPtrs conjuncts;
    collectConjuncts(predicate, conjuncts);

    ActionsDAG::NodeRawConstPtrs on_device;
    SplitPrewhere split;
    for (const auto * conjunct : conjuncts)
    {
        auto alone = ActionsDAG::buildFilterActionsDAG({conjunct});
        String refusal;
        auto compiled = alone ? GPU::compileGPUFilter(*alone, alone->getOutputs().front()->result_name, refusal) : std::nullopt;
        const bool readable = compiled
            && std::ranges::all_of(compiled->columns, [&](const NameAndTypePair & column) { return isDeviceReadableColumn(reading, column); });
        if (readable)
            on_device.push_back(conjunct);
        else
            split.on_host.push_back(conjunct);
    }

    if (on_device.empty())
        GPU_FILTERED_READ_REFUSE("a PREWHERE of which the device evaluates no conjunct");

    auto device_dag = ActionsDAG::buildFilterActionsDAG(on_device);
    String refusal;
    auto compiled = device_dag ? GPU::compileGPUFilter(*device_dag, device_dag->getOutputs().front()->result_name, refusal) : std::nullopt;
    if (!compiled)
        GPU_FILTERED_READ_REFUSE("the conjuncts of a PREWHERE the device evaluates one at a time, but not together: " + refusal);

    split.device = {
        .program = compiled->program,
        .columns = std::move(compiled->columns),
        .description = device_dag->getOutputs().front()->result_name,
    };
    split.on_device.insert(on_device.begin(), on_device.end());
    return split;
}

/// What the CPU computes of the `PREWHERE` over the rows the device sends back: the conjuncts the device leaves, and the
/// read's outputs. A conjunct the device evaluates that the read outputs, for a filter above, is a constant true there.
/// Returns the actions and the name of their filter column, if there is one.
std::pair<ActionsDAG, std::optional<String>> prewhereOnHost(const ActionsDAG & dag, const String & predicate_name, const SplitPrewhere & split)
{
    const ActionsDAG::Node * predicate = dag.tryFindInOutputs(predicate_name);

    ActionsDAG::NodeRawConstPtrs to_clone;
    for (const auto * output : dag.getOutputs())
    {
        if (output != predicate && !split.on_device.contains(withoutAliases(output)))
            to_clone.push_back(output);
    }
    to_clone.insert(to_clone.end(), split.on_host.begin(), split.on_host.end());

    ActionsDAG::NodeMapping mapping;
    ActionsDAG actions = ActionsDAG::cloneSubDAG(to_clone, mapping, /*remove_aliases=*/false);

    const ActionsDAG::Node * filter = nullptr;
    if (split.on_host.size() == 1)
    {
        filter = mapping.at(split.on_host.front());
    }
    else if (!split.on_host.empty())
    {
        ActionsDAG::NodeRawConstPtrs rest;
        for (const auto * conjunct : split.on_host)
            rest.push_back(mapping.at(conjunct));
        FunctionOverloadResolverPtr function_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        filter = &actions.addFunction(function_and, std::move(rest), {});
    }

    ActionsDAG::NodeRawConstPtrs outputs;
    for (const auto * output : dag.getOutputs())
    {
        if (output == predicate)
        {
            if (filter)
                outputs.push_back(filter);
            continue;
        }
        if (split.on_device.contains(withoutAliases(output)))
        {
            const ColumnConstPtr always_true = ColumnConst::create(output->result_type->createColumnConst(1, Field(UInt64(1)))->convertToFullColumnIfConst(), 0);
            outputs.push_back(&actions.addColumn(always_true, output->result_type, output->result_name));
            continue;
        }
        outputs.push_back(mapping.at(output));
    }
    actions.getOutputs() = std::move(outputs);

    return {std::move(actions), filter ? std::optional<String>(filter->result_name) : std::nullopt};
}

}

bool optimizeReadFilteredOnGPU(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    auto * reading = typeid_cast<ReadFromMergeTree *>(node.step.get());
    if (!reading)
        return false;

    const ContextPtr context = reading->getContext();
    if (!context->getSettingsRef()[Setting::allow_experimental_gpu_filtered_read])
        return false;

    if (settings.make_distributed_plan || settings.enable_parallel_replicas)
        GPU_FILTERED_READ_REFUSE("the plan is being made distributed");

    if (!readIsOfWholeParts(*reading))
        return false;

    /// The steps above a read in the order of the sorting key take its rows as sorted, and the device sends back the
    /// rows of the parts in no order.
    if (reading->readsInOrder() || reading->getInputOrder())
        GPU_FILTERED_READ_REFUSE("the read is in the order of the sorting key");

    const PrewhereInfoPtr prewhere = reading->getPrewhereInfo();
    if (!prewhere)
        GPU_FILTERED_READ_REFUSE("the read has no PREWHERE");

    /// By the column statistics, what the `PREWHERE` keeps: a read that keeps much of the table sends most of it back from
    /// the device, over the link it came by, and is faster on the CPU.
    {
        const auto max_selectivity = static_cast<double>(context->getSettingsRef()[Setting::gpu_filtered_read_max_selectivity]);
        const ActionsDAG::Node * predicate = prewhere->prewhere_actions.tryFindInOutputs(prewhere->prewhere_column_name);
        if (!predicate)
            GPU_FILTERED_READ_REFUSE("a PREWHERE without its predicate among its outputs");
        Names predicate_columns;
        for (const auto * input : prewhere->prewhere_actions.getInputs())
            predicate_columns.push_back(input->result_name);
        const auto estimator = reading->getConditionSelectivityEstimatorForPrewhere(predicate_columns, predicate);
        if (!estimator)
            GPU_FILTERED_READ_REFUSE("a PREWHERE whose selectivity there are no statistics to estimate");
        const UInt64 total_rows = estimator->getTotalRows();
        const UInt64 kept_rows = estimator->estimateRelationProfile(reading->getStorageMetadata(), predicate).rows;
        if (total_rows == 0 || static_cast<double>(kept_rows) > max_selectivity * static_cast<double>(total_rows))
            GPU_FILTERED_READ_REFUSE(fmt::format("a PREWHERE estimated to keep {} of {} rows, more than {} of them", kept_rows, total_rows, max_selectivity));
    }

    auto split = splitPrewhere(*reading);
    if (!split)
        return false;
    auto [on_host, filter_name] = prewhereOnHost(prewhere->prewhere_actions, prewhere->prewhere_column_name, *split);

    /// What the device sends back: the columns the actions on the host take, and the columns of the read the `PREWHERE`
    /// does not compute.
    const Block & read_header = *reading->getOutputHeader();
    ColumnsWithTypeAndName collected;
    NameSet collected_names;
    for (const auto * input : on_host.getInputs())
    {
        if (collected_names.insert(input->result_name).second)
            collected.emplace_back(input->result_type, input->result_name);
    }
    for (const auto & column : read_header)
    {
        if (!on_host.tryFindInOutputs(column.name) && collected_names.insert(column.name).second)
            collected.emplace_back(column.type, column.name);
    }
    if (collected.empty())
        GPU_FILTERED_READ_REFUSE("the read sends back no column");

    ReadColumns columns;
    for (const auto & column : collected)
    {
        const NameAndTypePair name_and_type(column.name, column.type);
        if (!isDeviceReadableColumn(*reading, name_and_type))
            GPU_FILTERED_READ_REFUSE("a column that is not a stored column the device reads: " + column.name);
        if (!GPU::collectElementTypeOf(*column.type))
            GPU_FILTERED_READ_REFUSE("a column of a type the device has no element type for: " + column.name);
        columns.columns.push_back({.column = name_and_type, .result_type = column.type, .aggregation = GPU::GPUAggregationKind::Sum});
    }

    if (!reading->getAnalyzedResult())
        reading->setAnalyzedResult(reading->selectRangesToRead());

    const auto analysis = reading->selectRangesToReadApplyingSkipIndexes();
    if (!analysis || !analysis->isUsable())
        GPU_FILTERED_READ_REFUSE("the read has no usable analyzed result");

    if (!queryLimitsAllowReading(*reading))
        return false;

    if (analysis->readFromProjection() || analysis->sampling.use_sampling || analysis->sampling.read_nothing)
        GPU_FILTERED_READ_REFUSE("the read is from a projection or sampled");

    auto parts = matchParts(*reading, analysis->parts_with_ranges, columns);
    if (!parts)
        return false;

    LOG_DEBUG(
        getLogger("GPUCompressedColumns"),
        "Filtering the read on the device: {} parts, {} columns sent back, a PREWHERE over {} columns, {} conjuncts of it on the CPU",
        parts->size(),
        columns.columns.size(),
        split->device.columns.size(),
        split->on_host.size());

    const auto source_header = std::make_shared<const Block>(Block(collected));
    auto & source_node = nodes.emplace_back();
    source_node.step = std::make_unique<ReadFromGPUCompressedColumns>(
        source_header,
        std::vector<NameAndTypePair>{},
        std::move(columns.columns),
        std::vector<ReadFromGPUCompressedColumns::DerivedColumn>{},
        std::vector<ReadFromGPUCompressedColumns::AggregateToCompute>{},
        std::move(split->device),
        std::move(*parts),
        reading->getStorageSnapshot(),
        context,
        context->getSettingsRef()[Setting::gpu_aggregation_batch_bytes],
        reading->getNumStreams(),
        context->getSettingsRef()[Setting::gpu_aggregation_readers],
        context->getSettingsRef()[Setting::gpu_aggregation_decompression],
        /*collect_=*/true);
    source_node.step->setStepDescription("Compressed columns decompressed and filtered on the device", settings.max_step_description_length);

    /// The rest of the `PREWHERE`, over the rows the device kept, and the columns the `PREWHERE` does not take, through it.
    for (const auto & column : *source_header)
    {
        if (!on_host.tryFindInOutputs(column.name) && std::ranges::none_of(on_host.getInputs(), [&](const auto * input) { return input->result_name == column.name; }))
            on_host.getOutputs().push_back(&on_host.addInput(column.name, column.type));
    }
    QueryPlanStepPtr on_host_step;
    if (filter_name)
        on_host_step = std::make_unique<FilterStep>(source_header, std::move(on_host), *filter_name, /*remove_filter_column_=*/true);
    else
        on_host_step = std::make_unique<ExpressionStep>(source_header, std::move(on_host));
    on_host_step->setStepDescription("The rest of the PREWHERE, on the CPU", settings.max_step_description_length);

    auto & on_host_node = nodes.emplace_back();
    on_host_node.children.push_back(&source_node);
    const SharedHeader on_host_header = on_host_step->getOutputHeader();
    on_host_node.step = std::move(on_host_step);

    /// In the read's header, which the steps above take.
    node.step = std::make_unique<ExpressionStep>(
        on_host_header,
        ActionsDAG::makeConvertingActions(
            on_host_header->getColumnsWithTypeAndName(), read_header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name, context));
    node.step->setStepDescription("The columns of the read", settings.max_step_description_length);
    node.children = {&on_host_node};
    return true;
}

}

#else

namespace DB::QueryPlanOptimizations
{

bool optimizeAggregationFromGPUCompressedColumns(QueryPlan::Node &, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &)
{
    return false;
}

bool optimizeReadFilteredOnGPU(QueryPlan::Node &, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &)
{
    return false;
}

}

#endif
