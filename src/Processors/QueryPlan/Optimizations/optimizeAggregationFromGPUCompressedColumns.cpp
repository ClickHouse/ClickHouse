#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include "config.h"

#if USE_GPU

#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/GPUAggregatingStep.h>
#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Access/EnabledRowPolicies.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <GPU/GPUDevice.h>
#include <GPU/GPUFilterCompiler.h>
#include <GPU/GPUTypeMapping.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Compression/ICompressionCodec.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <set>
#include <unordered_map>

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
    extern const SettingsUInt64 gpu_aggregation_batch_bytes;
    extern const SettingsUInt64 gpu_aggregation_readers;
    extern const SettingsFloat gpu_aggregation_device_decompression_max_ratio;
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

    Names arguments;
    arguments.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
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

struct MatchedChain
{
    QueryPlan::Node * above_read = nullptr;
    ReadFromMergeTree * reading = nullptr;
    std::unordered_map<String, String> read_names;
};

std::optional<MatchedChain> matchChain(QueryPlan::Node & aggregating_node, const Names & keys, const Names & arguments)
{
    MatchedChain matched;

    for (const auto & key : keys)
        matched.read_names[key] = key;
    for (const auto & argument : arguments)
        matched.read_names[argument] = argument;

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

        for (auto & [argument, name] : matched.read_names)
        {
            const auto input_name = passedThroughInputName(dag, name);
            if (!input_name)
                GPU_COMPRESSED_REFUSE("an expression that computes an aggregated column instead of passing it through");

            name = *input_name;
        }

        current = child;
    }
}

struct ReadColumns
{
    std::vector<NameAndTypePair> keys;
    std::vector<ReadFromGPUCompressedColumns::ColumnToReduce> columns;
};

std::optional<ReadColumns> matchReducedColumns(
    const GPUAggregatingStep & aggregating,
    const ReadFromMergeTree & reading,
    const std::unordered_map<String, String> & read_names,
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

        if (table_columns.hasCompressionCodec(name))
            GPU_COMPRESSED_REFUSE("a key column with a CODEC of its own");

        const auto [seen, inserted] = key_position_by_read_name.emplace(name, matched.keys.size());
        if (!inserted)
            GPU_COMPRESSED_REFUSE("a column that is a key twice over");

        matched.keys.emplace_back(name, read_column->type);
    }

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & aggregate = aggregates[i];
        const GPU::GPUAggregationKind aggregation = aggregations[i];

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

        if (!aggregate.function->getResultType()->equals(*type))
            GPU_COMPRESSED_REFUSE("an aggregate whose result type differs from its argument's");

        if (!GPU::canReduceOnDevice(*type, *type, aggregation))
            GPU_COMPRESSED_REFUSE("an aggregated column of a type the device does not reduce into itself");

        if (table_columns.hasCompressionCodec(name))
            GPU_COMPRESSED_REFUSE("an aggregated column with a CODEC of its own");

        if (key_position_by_read_name.contains(name))
            GPU_COMPRESSED_REFUSE("a column that is both a key and an aggregated column");

        const auto [seen, inserted] = aggregation_by_read_name.emplace(name, aggregation);
        if (!inserted && seen->second != aggregation)
            GPU_COMPRESSED_REFUSE("a column that two aggregates reduce by different aggregate functions");
    }

    matched.columns.reserve(read_header.columns());

    for (const auto & read_column : read_header)
    {
        if (!GPU::elementTypeOf(*read_column.type))
            GPU_COMPRESSED_REFUSE("a column of a type the device has no element type for");

        if (key_position_by_read_name.contains(read_column.name))
            continue;

        const auto aggregation = aggregation_by_read_name.find(read_column.name);
        if (aggregation == aggregation_by_read_name.end())
            GPU_COMPRESSED_REFUSE("the read produces a column nothing aggregates or groups by");

        matched.columns.push_back({
            .column = NameAndTypePair(read_column.name, read_column.type),
            .result_type = read_column.type,
            .aggregation = aggregation->second,
        });
    }

    if (!matched.keys.empty())
    {
        DataTypes key_types;
        DataTypes argument_types;
        std::vector<GPU::GPUAggregationKind> column_aggregations;
        for (const auto & key : matched.keys)
            key_types.push_back(key.type);
        for (const auto & column : matched.columns)
        {
            argument_types.push_back(column.column.type);
            column_aggregations.push_back(column.aggregation);
        }

        if (!GPU::canGroupByReduceOnDevice(key_types, argument_types, argument_types, column_aggregations))
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

        if (table_columns.hasCompressionCodec(column.name))
            GPU_COMPRESSED_REFUSE("a PREWHERE over a column with a CODEC of its own");
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
    /// read, which `matchWholeParts` sees; a `PREWHERE` is `matchPrewhere`'s to take or refuse.
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

std::optional<DataPartsVector> matchWholeParts(const ReadFromMergeTree & reading, const ReadColumns & columns)
{
    const MergeTreeData::MutationsSnapshotPtr & mutations = reading.getMutationsSnapshot();

    if (!mutations)
        GPU_COMPRESSED_REFUSE("the read has no mutations snapshot");

    if (mutations->hasDataMutations() || mutations->hasAlterMutations() || mutations->hasMetadataMutations()
        || mutations->hasPatchParts() || mutations->hasLightweightDeletedMask())
        GPU_COMPRESSED_REFUSE("the table has mutations, patch parts or a lightweight delete mask");

    if (reading.getMergeTreeData().hasEnabledMaskingPolicies(reading.getContext()))
        GPU_COMPRESSED_REFUSE("a masking policy applies to the table");

    const Block & read_header = *reading.getOutputHeader();

    DataPartsVector parts;
    parts.reserve(reading.getParts().size());

    for (const auto & part_with_ranges : reading.getParts())
    {
        const DataPartPtr & part = part_with_ranges.data_part;
        if (!part)
            GPU_COMPRESSED_REFUSE("a part without a data part object");

        if (part->isProjectionPart() || part_with_ranges.parent_part)
            GPU_COMPRESSED_REFUSE("a projection part");

        if (part_with_ranges.getRowsCount() != part->rows_count)
            GPU_COMPRESSED_REFUSE("a part of which less than every row is read");

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
        const auto part_stores = [&](const String & name)
        {
            const auto part_column = part->tryGetColumn(name);
            if (!part_column || !part_column->type->equals(*read_header.getByName(name).type))
                return false;
            if (!part->hasColumnFiles(*part_column))
                return false;
            return part->getSerialization(name)->getKindStack() == ISerialization::KindStack{ISerialization::Kind::DEFAULT};
        };

        for (const auto & key : columns.keys)
        {
            if (!part_stores(key.name))
                GPU_COMPRESSED_REFUSE("a part that does not store a key column with the table's type and the default serialization");
        }

        for (const auto & column : columns.columns)
        {
            if (!part_stores(column.column.name))
                GPU_COMPRESSED_REFUSE("a part that does not store an aggregated column with the table's type and the default serialization");
        }

        parts.push_back(part);
    }

    return parts;
}

}

bool optimizeAggregationFromGPUCompressedColumns(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
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

    if (!GPU::deviceProbeError().empty())
        GPU_COMPRESSED_REFUSE("no usable device");

    if (!readIsOfWholeParts(*reading))
        return false;

    auto columns = matchReducedColumns(*aggregating, *reading, chain->read_names, reduced->keys, reduced->aggregations);
    if (!columns)
        return false;

    auto filter = matchPrewhere(*reading, !columns->keys.empty());
    if (!filter)
        return false;

    if (!reading->getAnalyzedResult())
        reading->setAnalyzedResult(reading->selectRangesToRead());

    const auto analysis = reading->getAnalyzedResult();
    if (!analysis)
        GPU_COMPRESSED_REFUSE("the read has no analyzed result");

    if (!analysis->isUsable())
        GPU_COMPRESSED_REFUSE("the read exceeded its row limits");

    if (!queryLimitsAllowReading(*reading))
        return false;

    if (analysis->readFromProjection())
        GPU_COMPRESSED_REFUSE("the read is from a projection");

    if (analysis->total_marks_pk != analysis->selected_marks_pk)
        GPU_COMPRESSED_REFUSE("the primary key pruned marks");

    if (analysis->sampling.use_sampling || analysis->sampling.read_nothing)
        GPU_COMPRESSED_REFUSE("the read is sampled");

    auto parts = matchWholeParts(*reading, *columns);
    if (!parts)
        return false;

    LOG_DEBUG(
        getLogger("GPUCompressedColumns"),
        "Aggregating compressed columns on the device: {} whole parts, {} keys, {} columns{}",
        parts->size(),
        columns->keys.size(),
        columns->columns.size(),
        filter->filter ? ", a PREWHERE over " + std::to_string(filter->filter->columns.size()) + " columns" : "");

    SharedHeader header = reading->getOutputHeader();

    const SharedHeaders & headers_above_read = chain->above_read->step->getInputHeaders();
    if (headers_above_read.size() != 1 || !blocksHaveEqualStructure(*headers_above_read.front(), *header))
        GPU_COMPRESSED_REFUSE("the step above the read would see a different header");

    auto & source_node = nodes.emplace_back();
    const bool keyed = !columns->keys.empty();

    source_node.step = std::make_unique<ReadFromGPUCompressedColumns>(
        header,
        std::move(columns->keys),
        std::move(columns->columns),
        std::move(filter->filter),
        std::move(*parts),
        reading->getStorageSnapshot(),
        context,
        context->getSettingsRef()[Setting::gpu_aggregation_batch_bytes],
        reading->getNumStreams(),
        context->getSettingsRef()[Setting::gpu_aggregation_readers],
        context->getSettingsRef()[Setting::gpu_aggregation_device_decompression_max_ratio]);

    source_node.step->setStepDescription(
        keyed ? "Compressed columns decompressed and grouped on the device, one row per group"
              : "Compressed columns decompressed and reduced on the device, one row per part",
        settings.max_step_description_length);

    chain->above_read->children.front() = &source_node;

    /// The keyed read groups every part on the device and emits one row per group, so the
    /// aggregation above it has nothing left to group; a keyless read emits one row per part.
    if (keyed)
        typeid_cast<GPUAggregatingStep *>(node.step.get())->setInputGrouped();

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

}

#endif
