#include <optional>
#include <unordered_set>
#include <boost/rational.hpp> /// For calculations related to sampling coefficients.

#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/VirtualColumnUtils.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/Transforms/AggregatingTransform.h>

#include <Columns/FilterDescription.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/setThreadName.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/quoteString.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>


namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectExecutorThreads;
    extern const Metric MergeTreeDataSelectExecutorThreadsActive;
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled;
    extern const Metric FilteringMarksWithPrimaryKey;
    extern const Metric FilteringMarksWithSecondaryKeys;
}

namespace ProfileEvents
{
extern const Event FilteringMarksWithPrimaryKeyMicroseconds;
extern const Event FilteringMarksWithSecondaryKeysMicroseconds;
extern const Event IndexBinarySearchAlgorithm;
extern const Event IndexGenericExclusionSearchAlgorithm;
extern const Event FilterPartsByVirtualColumnsMicroseconds;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool per_part_index_stats;
    extern const SettingsBool allow_experimental_query_deduplication;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsString force_data_skipping_indices;
    extern const SettingsBool force_index_by_date;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_threads_for_indexes;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 merge_tree_coarse_index_granularity;
    extern const SettingsUInt64 merge_tree_min_bytes_for_seek;
    extern const SettingsUInt64 merge_tree_min_rows_for_seek;
    extern const SettingsUInt64 parallel_replica_offset;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsBool use_skip_indexes_for_disjunctions;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool secondary_indices_enable_bulk_filtering;
    extern const SettingsBool vector_search_with_rescoring;
    extern const SettingsBool use_skip_indexes_for_top_k;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INDEX_NOT_USED;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int ILLEGAL_COLUMN;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_PARSE_TEXT;
    extern const int TOO_MANY_ROWS;
    extern const int DUPLICATED_PART_UUIDS;
    extern const int INCORRECT_DATA;
}


MergeTreeDataSelectExecutor::MergeTreeDataSelectExecutor(const MergeTreeData & data_, ProjectionDescriptionRawPtr projection)
    : data(data_)
    , data_settings(data.getSettings(projection))
    , log(getLogger(data.getLogName() + " (SelectExecutor)"))
{
}

size_t MergeTreeDataSelectExecutor::getApproximateTotalRowsToRead(
    const RangesInDataParts & parts,
    const StorageMetadataPtr & metadata_snapshot,
    const KeyCondition & key_condition,
    const Settings & settings,
    LoggerPtr log)
{
    size_t rows_count = 0;

    /// We will find out how many rows we would have read without sampling.
    LOG_DEBUG(log, "Preliminary index scan with condition: {}", key_condition.toString());

    MarkRanges exact_ranges;
    for (const auto & part : parts)
    {
        MarkRanges part_ranges = markRangesFromPKRange(part, metadata_snapshot, key_condition, {}, {}, &exact_ranges, settings, log);
        for (const auto & range : part_ranges)
            rows_count += part.data_part->index_granularity->getRowsCountInRange(range);
    }
    UNUSED(exact_ranges);

    return rows_count;
}


using RelativeSize = boost::rational<ASTSampleRatio::BigNum>;

static std::string toString(const RelativeSize & x)
{
    return ASTSampleRatio::toString(x.numerator()) + "/" + ASTSampleRatio::toString(x.denominator());
}

/// Converts sample size to an approximate number of rows (ex. `SAMPLE 1000000`) to relative value (ex. `SAMPLE 0.1`).
static RelativeSize convertAbsoluteSampleSizeToRelative(const ASTSampleRatio::Rational & ratio, size_t approx_total_rows)
{
    if (approx_total_rows == 0)
        return 1;

    auto absolute_sample_size = ratio.numerator / ratio.denominator;
    return std::min(RelativeSize(1), RelativeSize(absolute_sample_size) / RelativeSize(approx_total_rows));
}

QueryPlanPtr MergeTreeDataSelectExecutor::read(
    const Names & column_names_to_return,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const UInt64 max_block_size,
    const size_t num_streams,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read,
    bool enable_parallel_reading) const
{
    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);

    auto step = readFromParts(
        snapshot_data.parts,
        snapshot_data.mutations_snapshot,
        column_names_to_return,
        storage_snapshot,
        query_info,
        context,
        max_block_size,
        num_streams,
        max_block_numbers_to_read,
        /*merge_tree_select_result_ptr=*/ nullptr,
        enable_parallel_reading);

    auto plan = std::make_unique<QueryPlan>();
    if (step)
        plan->addStep(std::move(step));
    return plan;
}

MergeTreeDataSelectSamplingData MergeTreeDataSelectExecutor::getSampling(
    const SelectQueryInfo & select_query_info,
    NamesAndTypesList available_real_columns,
    const RangesInDataParts & parts,
    KeyCondition & key_condition,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    LoggerPtr log)
{
    const Settings & settings = context->getSettingsRef();
    /// Sampling.
    MergeTreeDataSelectSamplingData sampling;

    RelativeSize relative_sample_size = 0;
    RelativeSize relative_sample_offset = 0;

    std::optional<ASTSampleRatio::Rational> sample_size_ratio;
    std::optional<ASTSampleRatio::Rational> sample_offset_ratio;

    if (select_query_info.table_expression_modifiers)
    {
        const auto & table_expression_modifiers = *select_query_info.table_expression_modifiers;
        sample_size_ratio = table_expression_modifiers.getSampleSizeRatio();
        sample_offset_ratio = table_expression_modifiers.getSampleOffsetRatio();
    }
    else
    {
        auto & select = select_query_info.query->as<ASTSelectQuery &>();

        auto select_sample_size = select.sampleSize();
        auto select_sample_offset = select.sampleOffset();

        if (select_sample_size)
            sample_size_ratio = select_sample_size->as<ASTSampleRatio &>().ratio;

        if (select_sample_offset)
            sample_offset_ratio = select_sample_offset->as<ASTSampleRatio &>().ratio;
    }

    if (sample_size_ratio)
    {
        relative_sample_size.assign(sample_size_ratio->numerator, sample_size_ratio->denominator);

        if (relative_sample_size < 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Negative sample size");

        relative_sample_offset = 0;
        if (sample_offset_ratio)
            relative_sample_offset.assign(sample_offset_ratio->numerator, sample_offset_ratio->denominator);

        if (relative_sample_offset < 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Negative sample offset");

        /// Convert absolute value of the sampling (in form `SAMPLE 1000000` - how many rows to
        /// read) into the relative `SAMPLE 0.1` (how much data to read).
        size_t approx_total_rows = 0;
        if (relative_sample_size > 1 || relative_sample_offset > 1)
            approx_total_rows = getApproximateTotalRowsToRead(parts, metadata_snapshot, key_condition, settings, log);

        if (relative_sample_size > 1)
        {
            relative_sample_size = convertAbsoluteSampleSizeToRelative(*sample_size_ratio, approx_total_rows);
            LOG_DEBUG(log, "Selected relative sample size: {}", toString(relative_sample_size));
        }

        /// SAMPLE 1 is the same as the absence of SAMPLE.
        if (relative_sample_size == RelativeSize(1))
            relative_sample_size = 0;

        if (relative_sample_offset > 0 && RelativeSize(0) == relative_sample_size)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Sampling offset is incorrect because no sampling");

        if (relative_sample_offset > 1)
        {
            relative_sample_offset = convertAbsoluteSampleSizeToRelative(*sample_offset_ratio, approx_total_rows);
            LOG_DEBUG(log, "Selected relative sample offset: {}", toString(relative_sample_offset));
        }
    }

    /** Which range of sampling key values do I need to read?
        * First, in the whole range ("universe") we select the interval
        *  of relative `relative_sample_size` size, offset from the beginning by `relative_sample_offset`.
        *
        * Example: SAMPLE 0.4 OFFSET 0.3
        *
        * [------********------]
        *        ^ - offset
        *        <------> - size
        *
        * If the interval passes through the end of the universe, then cut its right side.
        *
        * Example: SAMPLE 0.4 OFFSET 0.8
        *
        * [----------------****]
        *                  ^ - offset
        *                  <------> - size
        *
        * Next, if the `parallel_replicas_count`, `parallel_replica_offset` settings are set,
        *  then it is necessary to break the received interval into pieces of the number `parallel_replicas_count`,
        *  and select a piece with the number `parallel_replica_offset` (from zero).
        *
        * Example: SAMPLE 0.4 OFFSET 0.3, parallel_replicas_count = 2, parallel_replica_offset = 1
        *
        * [----------****------]
        *        ^ - offset
        *        <------> - size
        *        <--><--> - pieces for different `parallel_replica_offset`, select the second one.
        *
        * It is very important that the intervals for different `parallel_replica_offset` cover the entire range without gaps and overlaps.
        * It is also important that the entire universe can be covered using SAMPLE 0.1 OFFSET 0, ... OFFSET 0.9 and similar decimals.
        */

    const bool can_use_sampling_key_parallel_replicas =
        settings[Setting::allow_experimental_parallel_reading_from_replicas] > 0
        && settings[Setting::max_parallel_replicas] > 1
        && settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::SAMPLING_KEY;

    /// Parallel replicas has been requested but there is no way to sample data.
    /// Select all data from first replica and no data from other replicas.
    if (can_use_sampling_key_parallel_replicas && settings[Setting::parallel_replicas_count] > 1
        && !data.supportsSampling() && settings[Setting::parallel_replica_offset] > 0)
    {
        LOG_DEBUG(
            log,
            "Will use no data on this replica because parallel replicas processing has been requested"
            " (the setting 'max_parallel_replicas') but the table does not support sampling and this replica is not the first.");
        sampling.read_nothing = true;
        return sampling;
    }

    sampling.use_sampling = relative_sample_size > 0 || (can_use_sampling_key_parallel_replicas && settings[Setting::parallel_replicas_count] > 1 && data.supportsSampling());
    bool no_data = false; /// There is nothing left after sampling.

    if (sampling.use_sampling)
    {
        if (relative_sample_size != RelativeSize(0))
            sampling.used_sample_factor = 1.0 / boost::rational_cast<Float64>(relative_sample_size);

        RelativeSize size_of_universum = 0;
        const auto & sampling_key = metadata_snapshot->getSamplingKey();
        DataTypePtr sampling_column_type = sampling_key.data_types.at(0);

        if (sampling_key.data_types.size() == 1)
        {
            if (typeid_cast<const DataTypeUInt64 *>(sampling_column_type.get()))
                size_of_universum = RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1);
            else if (typeid_cast<const DataTypeUInt32 *>(sampling_column_type.get()))
                size_of_universum = RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1);
            else if (typeid_cast<const DataTypeUInt16 *>(sampling_column_type.get()))
                size_of_universum = RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1);
            else if (typeid_cast<const DataTypeUInt8 *>(sampling_column_type.get()))
                size_of_universum = RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1);
        }

        if (size_of_universum == RelativeSize(0))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Invalid sampling column type in storage parameters: {}. Must be one unsigned integer type",
                sampling_column_type->getName());

        if (settings[Setting::parallel_replicas_count] > 1)
        {
            if (relative_sample_size == RelativeSize(0))
                relative_sample_size = 1;

            relative_sample_size /= settings[Setting::parallel_replicas_count].value;
            relative_sample_offset += relative_sample_size * RelativeSize(settings[Setting::parallel_replica_offset].value);
        }

        if (relative_sample_offset >= RelativeSize(1))
            no_data = true;

        /// Calculate the half-interval of `[lower, upper)` column values.
        bool has_lower_limit = false;
        bool has_upper_limit = false;

        RelativeSize lower_limit_rational = relative_sample_offset * size_of_universum;
        RelativeSize upper_limit_rational = (relative_sample_offset + relative_sample_size) * size_of_universum;

        UInt64 lower = static_cast<UInt64>(boost::rational_cast<ASTSampleRatio::BigNum>(lower_limit_rational));
        UInt64 upper = static_cast<UInt64>(boost::rational_cast<ASTSampleRatio::BigNum>(upper_limit_rational));

        if (lower > 0)
            has_lower_limit = true;

        if (upper_limit_rational < size_of_universum)
            has_upper_limit = true;

        if ((has_upper_limit && upper == 0)
            || (has_lower_limit && has_upper_limit && lower == upper))
            no_data = true;

        if (no_data || (!has_lower_limit && !has_upper_limit))
        {
            sampling.use_sampling = false;
        }
        else
        {
            /// Let's add the conditions to cut off something else when the index is scanned again and when the request is processed.

            boost::intrusive_ptr<ASTFunction> lower_function;
            boost::intrusive_ptr<ASTFunction> upper_function;

            chassert(metadata_snapshot->getSamplingKeyAST() != nullptr);
            ASTPtr sampling_key_ast = metadata_snapshot->getSamplingKeyAST()->clone();

            if (has_lower_limit)
            {
                if (!key_condition.addCondition(
                        sampling_key.column_names[0],
                        Range::createLeftBounded(lower, true, isNullableOrLowCardinalityNullable(sampling_key.data_types[0]))))
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Sampling column not in primary key");

                ASTPtr args = make_intrusive<ASTExpressionList>();
                args->children.push_back(sampling_key_ast);
                args->children.push_back(make_intrusive<ASTLiteral>(lower));

                lower_function = make_intrusive<ASTFunction>();
                lower_function->name = "greaterOrEquals";
                lower_function->arguments = args;
                lower_function->children.push_back(lower_function->arguments);

                sampling.filter_function = lower_function;
            }

            if (has_upper_limit)
            {
                if (!key_condition.addCondition(
                        sampling_key.column_names[0],
                        Range::createRightBounded(upper, false, isNullableOrLowCardinalityNullable(sampling_key.data_types[0]))))
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Sampling column not in primary key");

                ASTPtr args = make_intrusive<ASTExpressionList>();
                args->children.push_back(sampling_key_ast);
                args->children.push_back(make_intrusive<ASTLiteral>(upper));

                upper_function = make_intrusive<ASTFunction>();
                upper_function->name = "less";
                upper_function->arguments = args;
                upper_function->children.push_back(upper_function->arguments);

                sampling.filter_function = upper_function;
            }

            if (has_lower_limit && has_upper_limit)
            {
                ASTPtr args = make_intrusive<ASTExpressionList>();
                args->children.push_back(lower_function);
                args->children.push_back(upper_function);

                sampling.filter_function = make_intrusive<ASTFunction>();
                sampling.filter_function->name = "and";
                sampling.filter_function->arguments = args;
                sampling.filter_function->children.push_back(sampling.filter_function->arguments);
            }

            ASTPtr query = sampling.filter_function;
            auto syntax_result = TreeRewriter(context).analyze(query, available_real_columns);
            sampling.filter_expression = std::make_shared<const ActionsDAG>(ExpressionAnalyzer(sampling.filter_function, syntax_result, context).getActionsDAG(false));
        }
    }

    if (no_data)
    {
        LOG_DEBUG(log, "Sampling yields no data.");
        sampling.read_nothing = true;
    }

    return sampling;
}

void MergeTreeDataSelectExecutor::buildKeyConditionFromPartOffset(
    std::optional<KeyCondition> & part_offset_condition, const ActionsDAG::Node * predicate, ContextPtr context)
{
    if (!predicate)
        return;

    auto part_offset_type = std::make_shared<DataTypeUInt64>();
    auto part_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    Block sample
        = {ColumnWithTypeAndName(part_offset_type->createColumn(), part_offset_type, "_part_offset"),
           ColumnWithTypeAndName(part_type->createColumn(), part_type, "_part")};

    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &sample, context);
    if (!dag)
        return;

    /// The _part filter should only be effective in conjunction with the _part_offset filter.
    auto required_columns = dag->getRequiredColumnsNames();
    if (std::find(required_columns.begin(), required_columns.end(), "_part_offset") == required_columns.end())
        return;

    part_offset_condition.emplace(KeyCondition{
        ActionsDAGWithInversionPushDown(dag->getOutputs().front(), context),
        context,
        sample.getNames(),
        std::make_shared<ExpressionActions>(ActionsDAG(sample.getColumnsWithTypeAndName()), ExpressionActionsSettings{}),
        {}});
}

void MergeTreeDataSelectExecutor::buildKeyConditionFromTotalOffset(
    std::optional<KeyCondition> & total_offset_condition, const ActionsDAG::Node * predicate, ContextPtr context)
{
    if (!predicate)
        return;

    auto part_offset_type = std::make_shared<DataTypeUInt64>();
    Block sample
        = {ColumnWithTypeAndName(part_offset_type->createColumn(), part_offset_type, "_part_offset"),
           ColumnWithTypeAndName(part_offset_type->createColumn(), part_offset_type, "_part_starting_offset")};

    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &sample, context);
    if (!dag)
        return;

    /// Try to recognize and fold expressions of the form:
    ///     _part_offset + _part_starting_offset
    /// or  _part_starting_offset + _part_offset
    ActionsDAG total_offset;
    const auto * part_offset = &total_offset.addInput("_part_offset", part_offset_type);
    const auto * part_starting_offset = &total_offset.addInput("_part_starting_offset", part_offset_type);
    const auto * node1
        = &total_offset.addFunction(FunctionFactory::instance().get("plus", context), {part_offset, part_starting_offset}, {});
    const auto * node2
        = &total_offset.addFunction(FunctionFactory::instance().get("plus", context), {part_starting_offset, part_offset}, {});
    auto matches = matchTrees({node1, node2}, *dag, false /* check_monotonicity */);
    auto new_inputs = resolveMatchedInputs(matches, {node1, node2}, dag->getOutputs());
    if (!new_inputs)
        return;
    dag = ActionsDAG::foldActionsByProjection(*new_inputs, dag->getOutputs());

    /// total_offset_condition is only valid if _part_offset and _part_starting_offset are used *together*.
    /// After folding, we expect a single input representing their combination.
    /// If more than one input remains, it means either of them is used independently,
    /// and we should skip adding total_offset_condition in that case.
    if (dag->getInputs().size() != 1)
        return;

    auto required_columns = dag->getRequiredColumns();
    total_offset_condition.emplace(KeyCondition{
        ActionsDAGWithInversionPushDown(dag->getOutputs().front(), context),
        context,
        required_columns.getNames(),
        std::make_shared<ExpressionActions>(ActionsDAG(required_columns), ExpressionActionsSettings{}),
        {}});
}

std::optional<std::unordered_set<String>> MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData & data,
    const RangesInDataParts & parts,
    const ActionsDAG::Node * predicate,
    ContextPtr context)
{
    if (!predicate)
        return {};

    auto sample = data.getHeaderWithVirtualsForFilter(metadata_snapshot);
    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &sample, context);
    if (!dag)
        return {};

    /// Check if the extracted DAG actually uses any virtual columns.
    /// If it only contains constants (e.g., "greater(45, 0)"), skip the parts filtering that can be expensive
    /// if there are many parts (because it does a linear search over all parts).
    bool has_virtual_column_input = false;
    for (const auto & input : dag->getInputs())
    {
        if (sample.has(input->result_name))
        {
            has_virtual_column_input = true;
            break;
        }
    }

    if (!has_virtual_column_input)
    {
        /// If no virtual columns are referenced the the filter is a constant expression.
        /// A constant-false filter (e.g. pushed down from a UNION ALL branch that can never
        /// match) must still exclude all parts, so check for that before skipping.
        const auto & output_node = *dag->getOutputs().front();
        if (output_node.column)
        {
            ConstantFilterDescription filter_description(*output_node.column);
            if (filter_description.always_false)
                return std::unordered_set<String>{};
        }
        return {};
    }

    auto start_time = std::chrono::steady_clock::now();

    auto virtual_columns_block = data.getBlockWithVirtualsForFilter(metadata_snapshot, parts);
    VirtualColumnUtils::filterBlockWithExpression(VirtualColumnUtils::buildFilterExpression(std::move(*dag), context), virtual_columns_block);
    auto result = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");

    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count();
    ProfileEvents::increment(ProfileEvents::FilterPartsByVirtualColumnsMicroseconds, elapsed_us);

    return result;
}

RangesInDataParts MergeTreeDataSelectExecutor::filterPartsByPartition(
    const RangesInDataParts & parts,
    const std::optional<PartitionPruner> & partition_pruner,
    const std::optional<KeyCondition> & minmax_idx_condition,
    const std::optional<std::unordered_set<String>> & part_values,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData & data,
    const ContextPtr & context,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    LoggerPtr log,
    ReadFromMergeTree::IndexStats & index_stats)
{
    RangesInDataParts res;
    const Settings & settings = context->getSettingsRef();
    DataTypes minmax_columns_types;

    if (metadata_snapshot->hasPartitionKey())
    {
        chassert(minmax_idx_condition && partition_pruner);
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        minmax_columns_types = MergeTreeData::getMinMaxColumnsTypes(partition_key);

        if (settings[Setting::force_index_by_date] && (minmax_idx_condition->alwaysUnknownOrTrue() && partition_pruner->isUseless()))
        {
            auto minmax_columns_names = MergeTreeData::getMinMaxColumnsNames(partition_key);
            throw Exception(ErrorCodes::INDEX_NOT_USED,
                "Neither MinMax index by columns ({}) nor partition expr is used and setting 'force_index_by_date' is set",
                fmt::join(minmax_columns_names, ", "));
        }
    }

    auto query_context = context->hasQueryContext() ? context->getQueryContext() : context;
    QueryStatusPtr query_status = context->getProcessListElement();

    PartFilterCounters part_filter_counters;
    if (query_context->getSettingsRef()[Setting::allow_experimental_query_deduplication])
        res = selectPartsToReadWithUUIDFilter(
            parts,
            part_values,
            data.getPinnedPartUUIDs(),
            minmax_idx_condition,
            minmax_columns_types,
            partition_pruner,
            max_block_numbers_to_read,
            query_context,
            part_filter_counters,
            log);
    else
        res = selectPartsToRead(
            parts,
            part_values,
            minmax_idx_condition,
            minmax_columns_types,
            partition_pruner,
            max_block_numbers_to_read,
            part_filter_counters,
            query_status);

    index_stats.emplace_back(ReadFromMergeTree::IndexStat{
        .type = ReadFromMergeTree::IndexType::None,
        .num_parts_after = part_filter_counters.num_initial_selected_parts,
        .num_granules_after = part_filter_counters.num_initial_selected_granules});

    if (minmax_idx_condition)
    {
        auto description = minmax_idx_condition->getDescription();
        index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::MinMax,
            .condition = std::move(description.condition),
            .used_keys = std::move(description.used_keys),
            .num_parts_after = part_filter_counters.num_parts_after_minmax,
            .num_granules_after = part_filter_counters.num_granules_after_minmax});
        LOG_DEBUG(log, "MinMax index condition: {}", minmax_idx_condition->toString());
    }

    if (partition_pruner)
    {
        auto description = partition_pruner->getKeyCondition().getDescription();
        index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::Partition,
            .condition = std::move(description.condition),
            .used_keys = std::move(description.used_keys),
            .num_parts_after = part_filter_counters.num_parts_after_partition_pruner,
            .num_granules_after = part_filter_counters.num_granules_after_partition_pruner});
    }

    return res;
}

RangesInDataParts MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(IndexAnalysisContext & filter_context, RangesInDataParts parts_with_ranges, ReadFromMergeTree::IndexStats & index_stats)
{
    auto & metadata_snapshot = filter_context.metadata_snapshot;
    auto & mutations_snapshot = filter_context.mutations_snapshot;
    const auto & query_info = filter_context.query_info;
    const auto & context = filter_context.context;
    const auto & key_condition = filter_context.indexes.key_condition;
    const auto & part_offset_condition = filter_context.indexes.part_offset_condition;
    const auto & total_offset_condition = filter_context.indexes.total_offset_condition;
    const auto & key_condition_rpn_template = filter_context.indexes.key_condition_rpn_template;
    const auto & skip_indexes = filter_context.indexes.skip_indexes;
    const auto & top_k_filter_info = filter_context.top_k_filter_info;
    const auto & reader_settings = filter_context.reader_settings;
    const auto & log = filter_context.log;
    size_t num_streams = filter_context.num_streams;
    bool use_skip_indexes = filter_context.indexes.use_skip_indexes;
    bool use_skip_indexes_for_disjunctions_ = filter_context.indexes.use_skip_indexes_for_disjunctions;
    bool use_skip_indexes_on_data_read_ = filter_context.indexes.use_skip_indexes_on_data_read;
    bool use_skip_indexes_if_final_exact_mode_ = filter_context.indexes.use_skip_indexes_if_final_exact_mode;
    bool find_exact_ranges = filter_context.find_exact_ranges;
    bool is_final_query = filter_context.query_info.isFinal();
    bool has_projections = filter_context.has_projections;
    auto & result = filter_context.result;

    const auto original_num_parts = parts_with_ranges.size();
    const Settings & settings = context->getSettingsRef();

    if (use_skip_indexes && settings[Setting::force_data_skipping_indices].changed)
    {
        const auto & indices_str = settings[Setting::force_data_skipping_indices].toString();
        auto forced_indices = parseIdentifiersOrStringLiterals(indices_str, settings);

        if (forced_indices.empty())
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "No indices parsed from force_data_skipping_indices ('{}')", indices_str);

        std::unordered_set<std::string> useful_indices_names;
        for (const auto & useful_index : skip_indexes.useful_indices)
            useful_indices_names.insert(useful_index.index->index.name);

        for (const auto & index_name : forced_indices)
        {
            if (!useful_indices_names.contains(index_name))
            {
                throw Exception(
                    ErrorCodes::INDEX_NOT_USED,
                    "Index {} is not used and setting 'force_data_skipping_indices' contains it",
                    backQuote(index_name));
            }
        }
    }

    struct IndexStat
    {
        std::atomic<size_t> total_granules = 0;
        std::atomic<size_t> granules_dropped = 0;
        std::atomic<size_t> total_parts = 0;
        std::atomic<size_t> parts_dropped = 0;
        std::atomic<size_t> elapsed_us = 0;
        std::atomic<MarkRanges::SearchAlgorithm> search_algorithm = MarkRanges::SearchAlgorithm::Unknown;
    };

    IndexStat pk_stat;

    size_t stat_size = skip_indexes.useful_indices.size();
    if (settings[Setting::per_part_index_stats])
    {
        stat_size = stat_size * parts_with_ranges.size();
    }

    std::vector<IndexStat> useful_indices_stat(stat_size);
    std::vector<IndexStat> merged_indices_stat(skip_indexes.merged_indices.size());

    std::atomic<size_t> sum_marks_pk = 0;
    std::atomic<size_t> sum_parts_pk = 0;
    std::atomic<size_t> top_k_elapsed_us = 0;

    std::atomic<size_t> sum_marks_union = 0;

    std::vector<size_t> skip_index_used_in_part(parts_with_ranges.size(), 0);

    std::vector<std::vector<MergeTreeIndexBulkGranulesMinMax::MinMaxGranule>> parts_top_k_granules(
                    parts_with_ranges.size(), std::vector<MergeTreeIndexBulkGranulesMinMax::MinMaxGranule>());

    bool perform_top_k_optimization = top_k_filter_info && skip_indexes.skip_index_for_top_k_filtering && !top_k_filter_info->where_clause;

    size_t num_threads = std::min<size_t>(num_streams, parts_with_ranges.size());
    if (settings[Setting::max_threads_for_indexes])
    {
        num_threads = std::min<size_t>(num_streams, settings[Setting::max_threads_for_indexes]);
    }

    /// NOTE: we must check the size of key_condition_rpn_template (not key_condition),
    /// because key_condition_rpn_template is the RPN used in mergePartialResultsForDisjunctions
    /// and in the callback that writes to the partial_disjunction_result bitset.
    /// When use_primary_key = false, key_condition has skip_analysis_ = true and its RPN
    /// has only 1 element, but the template has the full RPN which can exceed 32 elements.
    bool use_skip_indexes_for_disjunctions = use_skip_indexes_for_disjunctions_
                                && !use_skip_indexes_on_data_read_
                                && key_condition_rpn_template.has_value()
                                && key_condition_rpn_template->getRPN().size() <= MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT;

    auto is_index_supported_on_data_read = [&](const MergeTreeIndexPtr & index) -> bool
    {
        /// Vector similarity indexes are not applicable on data reads.
        if (index->isVectorSimilarityIndex())
            return false;

        return use_skip_indexes_on_data_read_;
    };

    /// Let's find what range to read from each part.
    {
        auto mark_cache = context->getIndexMarkCache();
        auto uncompressed_cache = context->getIndexUncompressedCache();
        auto vector_similarity_index_cache = context->getVectorSimilarityIndexCache();

        auto query_status = context->getProcessListElement();

        // These limits are checked per part so that we can fail very quickly
        // if we hit row limits on large datasets. Row counts use an atomic
        // counter as part processing typically uses multiple threads (max_threads)
        auto [limits, leaf_limits] = getRowLimits(settings, query_info);
        std::atomic<size_t> total_rows{0};

        auto process_part = [&](size_t part_index)
        {
            if (query_status)
                query_status->checkTimeLimit();

            auto & ranges = parts_with_ranges[part_index];
            if (metadata_snapshot->hasPrimaryKey() || part_offset_condition || total_offset_condition)
            {
                CurrentMetrics::Increment metric(CurrentMetrics::FilteringMarksWithPrimaryKey);
                ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithPrimaryKeyMicroseconds);

                size_t total_marks_count = ranges.data_part->index_granularity->getMarksCountWithoutFinal();
                pk_stat.total_parts.fetch_add(1, std::memory_order_relaxed);
                pk_stat.total_granules.fetch_add(ranges.data_part->index_granularity->getMarksCountWithoutFinal(), std::memory_order_relaxed);

                ranges.ranges = markRangesFromPKRange(
                    ranges,
                    metadata_snapshot,
                    key_condition,
                    part_offset_condition,
                    total_offset_condition,
                    find_exact_ranges ? &ranges.exact_ranges : nullptr,
                    settings,
                    log);

                pk_stat.search_algorithm.store(ranges.ranges.search_algorithm, std::memory_order_relaxed);
                pk_stat.granules_dropped.fetch_add(total_marks_count - ranges.ranges.getNumberOfMarks(), std::memory_order_relaxed);
                if (ranges.ranges.empty())
                    pk_stat.parts_dropped.fetch_add(1, std::memory_order_relaxed);
                pk_stat.elapsed_us.fetch_add(watch.elapsed(), std::memory_order_relaxed);
            }

            sum_marks_pk.fetch_add(ranges.getMarksCount(), std::memory_order_relaxed);

            if (!ranges.ranges.empty())
                sum_parts_pk.fetch_add(1, std::memory_order_relaxed);

            if (is_final_query && use_skip_indexes_if_final_exact_mode_)
            {
                ranges.ranges_snapshot_after_pk_analysis = ranges.ranges;
            }

            if (!skip_indexes.empty())
            {
                CurrentMetrics::Increment metric(CurrentMetrics::FilteringMarksWithSecondaryKeys);
                auto alter_conversions = MergeTreeData::getAlterConversionsForPart(ranges.data_part, mutations_snapshot, context);
                const auto & all_updated_columns = alter_conversions->getAllUpdatedColumns();

                auto can_use_index = [&](const MergeTreeIndexPtr & index) -> std::expected<void, PreformattedMessage>
                {
                    if (all_updated_columns.empty())
                        return {};

                    auto options = GetColumnsOptions(GetColumnsOptions::Kind::All).withSubcolumns();
                    auto required_columns_names = index ->getColumnsRequiredForIndexCalc();
                    auto required_columns_list = metadata_snapshot->getColumns().getByNames(options, required_columns_names);

                    auto it = std::ranges::find_if(required_columns_list, [&](const auto & column)
                    {
                        return all_updated_columns.contains(column.getNameInStorage());
                    });

                    if (it == required_columns_list.end())
                        return {};

                    return std::unexpected(
                        PreformattedMessage::create(
                            "Index {} is not used for part {} because it depends on column `{}` which will be updated on the fly",
                            index->index.name,
                            ranges.data_part->name,
                            it->getNameInStorage()));
                };

                auto can_use_merged_index = [&](const std::vector<MergeTreeIndexPtr> & indices) -> std::expected<void, PreformattedMessage>
                {
                    for (const auto & index : indices)
                    {
                        if (auto index_result = can_use_index(index); !index_result)
                            return index_result;
                    }
                    return {};
                };

                const auto num_indexes = skip_indexes.useful_indices.size();

                PartialDisjunctionResult partial_eval_results;
                if (use_skip_indexes_for_disjunctions)
                    partial_eval_results.resize(ranges.data_part->index_granularity->getMarksCountWithoutFinal() * MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT, true);

                for (size_t idx = 0; idx < num_indexes; ++idx)
                {
                    if (ranges.ranges.empty())
                        break;

                    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithSecondaryKeysMicroseconds);

                    const auto index_idx = skip_indexes.per_part_index_orders[part_index][idx];
                    const auto & index_and_condition = skip_indexes.useful_indices[index_idx];

                    auto index_stat_idx = idx;
                    if (settings[Setting::per_part_index_stats])
                        index_stat_idx += num_indexes * part_index;

                    auto & stat = useful_indices_stat[index_stat_idx];

                    stat.total_parts.fetch_add(1, std::memory_order_relaxed);
                    size_t total_granules = ranges.ranges.getNumberOfMarks();
                    stat.total_granules.fetch_add(total_granules, std::memory_order_relaxed);

                    if (auto index_result = can_use_index(index_and_condition.index); !index_result)
                    {
                        LOG_TRACE(log, "{}", index_result.error().text);
                        continue;
                    }

                    if (!is_index_supported_on_data_read(index_and_condition.index))
                    {
                        std::tie(ranges.ranges, ranges.read_hints) = filterMarksUsingIndex(
                            index_and_condition.index,
                            index_and_condition.condition,
                            key_condition_rpn_template,
                            ranges.data_part,
                            ranges.ranges,
                            ranges.read_hints,
                            reader_settings,
                            mark_cache.get(),
                            uncompressed_cache.get(),
                            vector_similarity_index_cache.get(),
                            use_skip_indexes_for_disjunctions,
                            partial_eval_results,
                            log);
                    }

                    stat.granules_dropped.fetch_add(total_granules - ranges.ranges.getNumberOfMarks(), std::memory_order_relaxed);
                    if (ranges.ranges.empty())
                        stat.parts_dropped.fetch_add(1, std::memory_order_relaxed);
                    stat.elapsed_us.fetch_add(watch.elapsed(), std::memory_order_relaxed);
                    skip_index_used_in_part[part_index] = 1; /// thread-safe
                }

                if (use_skip_indexes_for_disjunctions && key_condition_rpn_template.has_value())
                {
                    ranges.ranges = mergePartialResultsForDisjunctions(ranges.data_part,
                                        ranges.ranges, key_condition_rpn_template.value(),
                                        partial_eval_results, reader_settings, log);

                    sum_marks_union.fetch_add(ranges.getMarksCount(), std::memory_order_relaxed);
                }

                for (size_t idx = 0; idx < skip_indexes.merged_indices.size(); ++idx)
                {
                    if (ranges.ranges.empty())
                        break;

                    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithSecondaryKeysMicroseconds);

                    const auto & indices_and_condition = skip_indexes.merged_indices[idx];
                    auto & stat = merged_indices_stat[idx];
                    stat.total_parts.fetch_add(1, std::memory_order_relaxed);

                    if (auto index_result = can_use_merged_index(indices_and_condition.indices); !index_result)
                    {
                        LOG_TRACE(log, "{}", index_result.error().text);
                        continue;
                    }

                    size_t total_granules = ranges.ranges.getNumberOfMarks();

                    if (!use_skip_indexes_on_data_read_)
                    {
                        ranges.ranges = filterMarksUsingMergedIndex(
                            indices_and_condition.indices,
                            indices_and_condition.condition,
                            ranges.data_part,
                            ranges.ranges,
                            reader_settings,
                            mark_cache.get(),
                            uncompressed_cache.get(),
                            vector_similarity_index_cache.get(),
                            log);
                    }

                    stat.total_granules.fetch_add(total_granules, std::memory_order_relaxed);
                    stat.granules_dropped.fetch_add(total_granules - ranges.ranges.getNumberOfMarks(), std::memory_order_relaxed);

                    if (ranges.ranges.empty())
                        stat.parts_dropped.fetch_add(1, std::memory_order_relaxed);
                }
            }

            /// Optimize ORDER BY <col> LIMIT n - if <col> is scalar numeric / date / datetime and has a minmax index
            if (perform_top_k_optimization)
            {
                ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilteringMarksWithSecondaryKeysMicroseconds);

                auto min_max_granules = getMinMaxIndexGranules(ranges.data_part,
                                            skip_indexes.skip_index_for_top_k_filtering,
                                            ranges.ranges,
                                            top_k_filter_info->direction,
                                            false, /*access_by_mark*/
                                            reader_settings,
                                            mark_cache.get(),
                                            uncompressed_cache.get(),
                                            vector_similarity_index_cache.get());

                if (min_max_granules) /// minmax index may have not been materialized for this part, not a fatal error
                {
                    min_max_granules->getTopKMarks(top_k_filter_info->limit_n, parts_top_k_granules[part_index]);
                }
                top_k_elapsed_us.fetch_add(watch.elapsed(), std::memory_order_relaxed);
            }

            if (!ranges.ranges.empty() && !perform_top_k_optimization)
            {
                if (limits.max_rows || leaf_limits.max_rows)
                {
                    auto current_rows_estimate = ranges.getRowsCount();
                    size_t prev_rows_estimate = total_rows.fetch_add(current_rows_estimate, std::memory_order_relaxed);
                    size_t total_rows_estimate = current_rows_estimate + prev_rows_estimate;

                    if (query_info.trivial_limit > 0 && total_rows_estimate > query_info.trivial_limit)
                    {
                        total_rows_estimate = query_info.trivial_limit;
                    }

                    bool exceeds_limits = (limits.max_rows && total_rows_estimate > limits.max_rows)
                                       || (leaf_limits.max_rows && total_rows_estimate > leaf_limits.max_rows);

                    /// Even though we exceeded limits on parent parts, we may be able to use a projection
                    /// mark the result as unusable and return gracefully to analyze projection candidates
                    if (exceeds_limits && has_projections)
                    {
                        result.exceeded_row_limits = true;
                        return;
                    }

                    limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
                    leaf_limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
                }
            }
        };

        LOG_TRACE(log, "Filtering marks by primary and secondary keys");

        if (num_threads <= 1)
        {
            for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
                process_part(part_index);
        }
        else
        {
            /// Parallel loading and filtering of data parts.
            ThreadPool pool(
                CurrentMetrics::MergeTreeDataSelectExecutorThreads,
                CurrentMetrics::MergeTreeDataSelectExecutorThreadsActive,
                CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
                num_threads);


            /// Instances of ThreadPool "borrow" threads from the global thread pool.
            /// We intentionally use scheduleOrThrow here to avoid a deadlock.
            /// For example, queries can already be running with threads from the
            /// global pool, and if we saturate max_thread_pool_size whilst requesting
            /// more in this loop, queries will block infinitely.
            /// So we wait until lock_acquire_timeout, and then raise an exception.
            for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
            {
                pool.scheduleOrThrow(
                    [&, part_index, thread_group = CurrentThread::getGroup()]
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_INDEX);

                        process_part(part_index);
                    },
                    Priority{},
                    context->getSettingsRef()[Setting::lock_acquire_timeout].totalMicroseconds());
            }

            pool.wait();
        }

    }

    /// merge top-N results if optimization is done
    size_t sum_marks_after_top_k = 0;
    size_t sum_parts_after_top_k = 0;
    if (perform_top_k_optimization)
    {
        std::vector<MarkRanges> top_k_granules_result;
        MergeTreeIndexBulkGranulesMinMax::getTopKMarks(top_k_filter_info->direction, top_k_filter_info->limit_n,
                                                       parts_top_k_granules, top_k_granules_result);
        for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
        {
            if (!parts_top_k_granules[part_index].empty())
            {
                parts_with_ranges[part_index].ranges = std::move(top_k_granules_result[part_index]);
            }
            if (!parts_with_ranges[part_index].ranges.empty())
            {
                sum_parts_after_top_k++;
                sum_marks_after_top_k += parts_with_ranges[part_index].ranges.getNumberOfMarks();
            }
        }
    }

    if (metadata_snapshot->hasPrimaryKey())
    {
        LOG_DEBUG(
            log,
            "PK index has dropped {}/{} granules, it took {}ms across {} threads.",
            pk_stat.granules_dropped.load(),
            pk_stat.total_granules.load(),
            pk_stat.elapsed_us.load() / 1000,
            num_threads);

        auto description = key_condition.getDescription();

        index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::PrimaryKey,
            .condition = std::move(description.condition),
            .used_keys = std::move(description.used_keys),
            .num_parts_after = sum_parts_pk.load(std::memory_order_relaxed),
            .num_granules_after = sum_marks_pk.load(std::memory_order_relaxed),
            .search_algorithm = pk_stat.search_algorithm.load(std::memory_order_relaxed)
        });
    }

    const auto num_indices = skip_indexes.useful_indices.size();

    const auto part_stats_granularity = settings[Setting::per_part_index_stats] ? original_num_parts : 1;
    for (size_t part_index = 0; part_index < part_stats_granularity; ++part_index)
    {
        for (size_t idx = 0; idx < skip_indexes.useful_indices.size(); ++idx)
        {
            const auto & stat = useful_indices_stat[part_index * num_indices + idx];
            const auto & index_and_condition = skip_indexes.useful_indices[skip_indexes.per_part_index_orders[part_index][idx]];
            const auto & index_name = index_and_condition.index->index.name;
            LOG_DEBUG(
                log,
                "Index {} has dropped {}/{} granules, it took {}ms across {} threads.",
                backQuote(index_name),
                stat.granules_dropped.load(),
                stat.total_granules.load(),
                stat.elapsed_us.load() / 1000,
                num_threads);

            std::string description = index_and_condition.index->index.type + " GRANULARITY " + std::to_string(index_and_condition.index->index.granularity);
            if (settings[Setting::per_part_index_stats])
            {
                description += " PART " + std::to_string(part_index) + "/" + std::to_string(part_stats_granularity);
                index_stats.emplace_back(ReadFromMergeTree::IndexStat{
                    .type = ReadFromMergeTree::IndexType::Skip,
                    .name = index_name,
                    .part_name = parts_with_ranges[part_index].data_part->name,
                    .description = std::move(description),
                    .condition = index_and_condition.condition->getDescription(),
                    .num_parts_after = stat.total_parts - stat.parts_dropped,
                    .num_granules_after = stat.total_granules - stat.granules_dropped});
            }
            else
            {
                index_stats.emplace_back(ReadFromMergeTree::IndexStat{
                    .type = ReadFromMergeTree::IndexType::Skip,
                    .name = index_name,
                    .description = std::move(description),
                    .condition = index_and_condition.condition->getDescription(),
                    .num_parts_after = stat.total_parts - stat.parts_dropped,
                    .num_granules_after = stat.total_granules - stat.granules_dropped});
            }
        }

        if (use_skip_indexes_for_disjunctions)
        {
            index_stats.emplace_back(ReadFromMergeTree::IndexStat{
                .type = ReadFromMergeTree::IndexType::Skip,
                .name = "<Combined skip indexes>",
                .description = "Final set of granules after AND/OR processing",
                .num_parts_after = sum_parts_pk.load(std::memory_order_relaxed),
                .num_granules_after = sum_marks_union.load(std::memory_order_relaxed)});
        }
    }

    for (size_t idx = 0; idx < skip_indexes.merged_indices.size(); ++idx)
    {
        const auto & index_and_condition = skip_indexes.merged_indices[idx];
        const auto & stat = merged_indices_stat[idx];
        const auto & index_name = "Merged";
        LOG_DEBUG(
            log,
            "Index {} has dropped {}/{} granules, it took {}ms across {} threads.",
            backQuote(index_name),
            stat.granules_dropped.load(),
            stat.total_granules.load(),
            stat.elapsed_us.load() / 1000,
            num_threads);

        std::string description = "MERGED GRANULARITY " + std::to_string(index_and_condition.indices.at(0)->index.granularity);

        index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::Skip,
            .name = index_name,
            .description = std::move(description),
            .num_parts_after = stat.total_parts - stat.parts_dropped,
            .num_granules_after = stat.total_granules - stat.granules_dropped});
    }

    if (perform_top_k_optimization)
    {
        index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::Skip,
            .name = skip_indexes.skip_index_for_top_k_filtering->index.name,
            .description = "Filter TopK Granules",
            .num_parts_after = sum_parts_after_top_k,
            .num_granules_after = sum_marks_after_top_k});
        LOG_DEBUG(
            log,
            "Skip index {} used for top N optimization, it took {}ms across {} threads.",
            skip_indexes.skip_index_for_top_k_filtering->index.name,
            top_k_elapsed_us.load() / 1000,
            num_threads);
    }
    /// Skip empty ranges.
    std::erase_if(
        parts_with_ranges,
        [&](const auto & part)
        {
            size_t index = &part - parts_with_ranges.data();
            if (is_final_query && use_skip_indexes_if_final_exact_mode_ && skip_index_used_in_part[index])
            {
                /// retain this part even if empty due to FINAL
                return false;
            }

            return !part.data_part || part.ranges.empty();
        });


    return parts_with_ranges;
}

MergeTreeDataSelectExecutor::RowLimits MergeTreeDataSelectExecutor::getRowLimits(
    const Settings & settings,
    const SelectQueryInfo & query_info)
{
    RowLimits row_limits;

    /// Do not check number of read rows if we are
    /// reading in order via the sorting key with limit.
    /// In the general case, when you have a WHERE clause
    /// it's impossible to estimate number of rows precisely,
    /// because we can stop reading at any time, especially given
    /// part processing is done in multiple threads (max_threads)
    if (settings[Setting::read_overflow_mode] == OverflowMode::THROW
        && settings[Setting::max_rows_to_read]
        && !query_info.input_order_info)
        row_limits.limits = SizeLimits(settings[Setting::max_rows_to_read], 0, settings[Setting::read_overflow_mode]);

    if (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW
        && settings[Setting::max_rows_to_read_leaf]
        && !query_info.input_order_info)
        row_limits.leaf_limits = SizeLimits(settings[Setting::max_rows_to_read_leaf], 0, settings[Setting::read_overflow_mode_leaf]);

    return row_limits;
}

void MergeTreeDataSelectExecutor::filterPartsByQueryConditionCache(
    RangesInDataParts & parts_with_ranges,
    const SelectQueryInfo & select_query_info,
    const std::optional<VectorSearchParameters> & vector_search_parameters,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    const ContextPtr & context,
    LoggerPtr log)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::use_query_condition_cache]
            || !settings[Setting::allow_experimental_analyzer]
            || (!select_query_info.prewhere_info && !select_query_info.filter_actions_dag)
            || (vector_search_parameters.has_value()) /// vector search has filter in the ORDER BY
            || select_query_info.isFinal()
            || (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasPatchParts()))
        return;

    QueryConditionCachePtr query_condition_cache = context->getQueryConditionCache();

    struct Stats
    {
        size_t total_granules = 0;
        size_t granules_dropped = 0;
    };

    auto drop_mark_ranges = [&](const ActionsDAG::Node * dag)
    {
        UInt64 condition_hash = dag->getHash();
        Stats stats;
        for (auto it = parts_with_ranges.begin(); it != parts_with_ranges.end();)
        {
            auto & part_with_ranges = *it;
            stats.total_granules += part_with_ranges.getMarksCount();

            const auto & data_part = part_with_ranges.data_part;
            auto storage_id = data_part->storage.getStorageID();
            auto matching_marks_opt = query_condition_cache->read(storage_id.uuid, data_part->name, condition_hash);
            if (!matching_marks_opt)
            {
                ++it;
                continue;
            }

            auto & matching_marks = *matching_marks_opt;
            MarkRanges ranges;
            const auto & part = it->data_part;
            size_t min_marks_for_seek = roundRowsOrBytesToMarks(
                settings[Setting::merge_tree_min_rows_for_seek],
                settings[Setting::merge_tree_min_bytes_for_seek],
                part->index_granularity_info.fixed_index_granularity,
                part->index_granularity_info.index_granularity_bytes);

            for (const auto & mark_range : part_with_ranges.ranges)
            {
                size_t begin = mark_range.begin;
                for (size_t mark_it = begin; mark_it < mark_range.end;)
                {
                    if (!matching_marks[mark_it])
                    {
                        if (mark_it == begin)
                        {
                            /// mark_range.begin -> 0 0 0 1 x x x x. Need to skip starting zeros.
                            ++stats.granules_dropped;
                            ++begin;
                            ++mark_it;
                        }
                        else
                        {
                            size_t end = mark_it;
                            for (; end < mark_range.end && !matching_marks[end]; ++end)
                                ;

                            if (min_marks_for_seek && end != mark_range.end && end - mark_it <= min_marks_for_seek)
                            {
                                /// x x x 1 1 1 0 0 1 x x x. And gap is small enough to merge, skip gap.
                                mark_it = end + 1;
                            }
                            else
                            {
                                /// Case1: x x x 1 1 1 0 0 1 x x x. Gap is too big to merge, do not merge
                                /// Case2: x x x 1 1 1 0 0 0 0 -> mark_range.end. Reach the end of range, do not merge
                                stats.granules_dropped += end - mark_it;
                                ranges.emplace_back(begin, mark_it);
                                begin = end;

                                if (end == mark_range.end)
                                    break;

                                mark_it = end + 1;
                            }
                        }
                    }
                    else
                        ++mark_it;
                }

                if (begin != mark_range.begin && begin != mark_range.end)
                    ranges.emplace_back(begin, mark_range.end);
                else if (begin == mark_range.begin)
                    ranges.emplace_back(begin, mark_range.end);
            }

            if (ranges.empty())
                it = parts_with_ranges.erase(it);
            else
            {
                part_with_ranges.ranges = std::move(ranges);
                ++it;
            }
        }

        return stats;
    };

    if (const auto & prewhere_info = select_query_info.prewhere_info)
    {
        for (const auto * outputs : prewhere_info->prewhere_actions.getOutputs())
        {
            if (outputs->result_name == prewhere_info->prewhere_column_name)
            {
                auto stats = drop_mark_ranges(outputs);
                LOG_DEBUG(log,
                        "Query condition cache has dropped {}/{} granules for PREWHERE condition {}.",
                        stats.granules_dropped,
                        stats.total_granules,
                        prewhere_info->prewhere_column_name);
                break;
            }
        }
    }

    if (const auto & filter_actions_dag = select_query_info.filter_actions_dag)
    {
        const auto * output = filter_actions_dag->getOutputs().front();
        auto stats = drop_mark_ranges(output);
        LOG_DEBUG(log,
                "Query condition cache has dropped {}/{} granules for WHERE condition {}.",
                stats.granules_dropped,
                stats.total_granules,
                filter_actions_dag->getOutputs().front()->result_name);
    }
}

ReadFromMergeTree::AnalysisResultPtr MergeTreeDataSelectExecutor::estimateNumMarksToRead(
    RangesInDataParts parts,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    const Names & column_names_to_return,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    size_t num_streams,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read) const
{
    size_t total_parts = parts.size();
    if (total_parts == 0)
        return std::make_shared<ReadFromMergeTree::AnalysisResult>();

    std::optional<ReadFromMergeTree::Indexes> indexes;
    return ReadFromMergeTree::selectRangesToRead(
        parts,
        mutations_snapshot,
        std::nullopt,
        std::nullopt,
        metadata_snapshot,
        query_info,
        context,
        num_streams,
        max_block_numbers_to_read,
        data,
        data_settings,
        column_names_to_return,
        log,
        indexes,
        /*find_exact_ranges*/false,
        /*is_parallel_reading_from_replicas*/false,
        /*use_query_condition_cache*/true,
        /*supports_skip_indexes_on_data_read*/false);
}

QueryPlanStepPtr MergeTreeDataSelectExecutor::readFromParts(
    RangesInDataPartsPtr parts,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    const Names & column_names_to_return,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const UInt64 max_block_size,
    const size_t num_streams,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read,
    ReadFromMergeTree::AnalysisResultPtr merge_tree_select_result_ptr,
    bool enable_parallel_reading,
    std::shared_ptr<ParallelReadingExtension> extension_) const
{
    /// If merge_tree_select_result_ptr != nullptr, we use analyzed result so parts will always be empty.
    if (merge_tree_select_result_ptr)
    {
        if (merge_tree_select_result_ptr->parts_with_ranges.empty())
            return {};
    }
    /// If merge_tree_enable_remove_parts_from_snapshot_optimization is true it nukes our list of parts
    else if (!parts)
        return {};
    else if (parts->empty())
        return {};

    return std::make_unique<ReadFromMergeTree>(
        parts,
        std::move(mutations_snapshot),
        column_names_to_return,
        data,
        data_settings,
        query_info,
        storage_snapshot,
        context,
        max_block_size,
        num_streams,
        max_block_numbers_to_read,
        log,
        merge_tree_select_result_ptr,
        enable_parallel_reading,
        extension_ ? std::optional(extension_->getAllRangesCallback()) : std::nullopt,
        extension_ ? std::optional(extension_->getReadTaskCallback()) : std::nullopt,
        extension_ ? std::optional(extension_->getNumberOfCurrentReplica()) : std::nullopt);
}


/// Marks are placed whenever threshold on rows or bytes is met.
/// So we have to return the number of marks on whatever estimate is higher - by rows or by bytes.
size_t MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
    size_t rows_setting,
    size_t bytes_setting,
    size_t rows_granularity,
    size_t bytes_granularity)
{
    size_t res = (rows_setting + rows_granularity - 1) / rows_granularity;

    if (bytes_granularity == 0)
        return res;
    return std::max(res, (bytes_setting + bytes_granularity - 1) / bytes_granularity);
}

/// Same as roundRowsOrBytesToMarks() but do not return more then max_marks
size_t MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
    size_t rows_setting, size_t bytes_setting, size_t rows_granularity, size_t bytes_granularity, size_t min_marks, size_t max_marks)
{
    size_t marks = 1;

    if (rows_setting + rows_granularity <= rows_setting) /// overflow
        marks = max_marks;
    else if (rows_setting)
        marks = (rows_setting + rows_granularity - 1) / rows_granularity;

    if (bytes_granularity)
    {
        /// Overflow
        if (bytes_setting + bytes_granularity <= bytes_setting) /// overflow
            marks = max_marks;
        else if (bytes_setting)
            marks = std::max(marks, (bytes_setting + bytes_granularity - 1) / bytes_granularity);
    }
    return std::max(marks, min_marks);
}

/// Calculates a set of mark ranges, that could possibly contain keys, required by condition.
/// In other words, it removes subranges from whole range, that definitely could not contain required keys.
/// If @exact_ranges is not null, fill it with ranges containing marks of fully matched records.
MarkRanges MergeTreeDataSelectExecutor::markRangesFromPKRange(
    const RangesInDataPart & part_with_ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const KeyCondition & key_condition,
    const std::optional<KeyCondition> & part_offset_condition,
    const std::optional<KeyCondition> & total_offset_condition,
    MarkRanges * exact_ranges,
    const Settings & settings,
    LoggerPtr log)
{
    const auto & part = part_with_ranges.data_part;
    MarkRanges res;

    size_t marks_count = part->index_granularity->getMarksCount();
    if (marks_count == 0)
        return res;

    bool key_condition_useful = !key_condition.alwaysUnknownOrTrue();
    bool part_offset_condition_useful = part_offset_condition && !part_offset_condition->alwaysUnknownOrTrue();
    bool total_offset_condition_useful = total_offset_condition && !total_offset_condition->alwaysUnknownOrTrue();

    /// If index is not used.
    if (!key_condition_useful && !part_offset_condition_useful && !total_offset_condition_useful)
        return part_with_ranges.ranges;

    /// If conditions are relaxed, don't fill exact ranges.
    if (key_condition.isRelaxed() || (part_offset_condition && part_offset_condition->isRelaxed())
        || (total_offset_condition && total_offset_condition->isRelaxed()))
        exact_ranges = nullptr;

    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const auto & sorting_key = metadata_snapshot->getSortingKey();
    auto index_columns = std::make_shared<ColumnsWithTypeAndName>();
    std::vector<bool> reverse_flags;
    size_t num_key_columns = key_condition.getNumKeyColumns();
    DataTypes key_types;
    if (num_key_columns > 0)
    {
        const auto index = part->getIndex();

        for (size_t i = 0; i < num_key_columns; ++i)
        {
            if (i < index->size())
            {
                index_columns->emplace_back(index->at(i), primary_key.data_types[i], primary_key.column_names[i]);
                reverse_flags.push_back(!sorting_key.reverse_flags.empty() && sorting_key.reverse_flags[i]);
            }
            else
            {
                index_columns->emplace_back(); /// The column of the primary key was not loaded in memory - we'll skip it.
                reverse_flags.push_back(false);
            }

            key_types.emplace_back(primary_key.data_types[i]);
        }
    }

    /// If there are no monotonic functions, there is no need to save block reference.
    /// Passing explicit field to FieldRef allows to optimize ranges and shows better performance.
    std::function<void(size_t, size_t, FieldRef &)> create_field_ref;
    if (key_condition.hasMonotonicFunctionsChain())
    {
        create_field_ref = [index_columns](size_t row, size_t column, FieldRef & field)
        {
            field = {index_columns.get(), row, column};
            // NULL_LAST
            if (field.isNull())
                field = POSITIVE_INFINITY;
        };
    }
    else
    {
        create_field_ref = [index_columns](size_t row, size_t column, FieldRef & field)
        {
            (*index_columns)[column].column->get(row, field);
            // NULL_LAST
            if (field.isNull())
                field = POSITIVE_INFINITY;
        };
    }

    /// NOTE Creating temporary Field objects to pass to KeyCondition.
    size_t used_key_size = num_key_columns;
    std::vector<FieldRef> index_left(used_key_size);
    std::vector<FieldRef> index_right(used_key_size);

    /// For _part_offset and _part virtual columns
    DataTypes part_offset_types
        = {std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())};
    std::vector<FieldRef> part_offset_left(2);
    std::vector<FieldRef> part_offset_right(2);

    auto check_in_range = [&](const MarkRange & range, BoolMask initial_mask = {})
    {
        auto check_key_condition = [&]()
        {
            if (range.end == marks_count)
            {
                for (size_t i = 0; i < used_key_size; ++i)
                {
                    auto & left = reverse_flags[i] ? index_right[i] : index_left[i];
                    auto & right = reverse_flags[i] ? index_left[i] : index_right[i];
                    if ((*index_columns)[i].column)
                        create_field_ref(range.begin, i, left);
                    else
                        left = NEGATIVE_INFINITY;

                    right = POSITIVE_INFINITY;
                }
            }
            else
            {
                for (size_t i = 0; i < used_key_size; ++i)
                {
                    auto & left = reverse_flags[i] ? index_right[i] : index_left[i];
                    auto & right = reverse_flags[i] ? index_left[i] : index_right[i];
                    if ((*index_columns)[i].column)
                    {
                        create_field_ref(range.begin, i, left);
                        create_field_ref(range.end, i, right);
                    }
                    else
                    {
                        /// If the PK column was not loaded in memory - exclude it from the analysis.
                        left = NEGATIVE_INFINITY;
                        right = POSITIVE_INFINITY;
                    }
                }
            }
            return key_condition.checkInRange(used_key_size, index_left.data(), index_right.data(), key_types, initial_mask);
        };

        auto check_part_offset_condition = [&]()
        {
            auto begin = part->index_granularity->getMarkStartingRow(range.begin);
            auto end = part->index_granularity->getMarkStartingRow(range.end) - 1;
            if (begin > end)
            {
                /// Empty mark (final mark)
                return BoolMask(false, true);
            }

            part_offset_left[0] = begin;
            part_offset_right[0] = end;

            part_offset_left[1] = part->name;
            part_offset_right[1] = part->name;

            return part_offset_condition->checkInRange(
                2, part_offset_left.data(), part_offset_right.data(), part_offset_types, initial_mask);
        };

        auto check_total_offset_condition = [&]()
        {
            auto begin = part->index_granularity->getMarkStartingRow(range.begin);
            auto end = part->index_granularity->getMarkStartingRow(range.end) - 1;
            if (begin > end)
            {
                /// Empty mark (final mark)
                return BoolMask(false, true);
            }

            part_offset_left[0] = begin + part_with_ranges.part_starting_offset_in_query;
            part_offset_right[0] = end + part_with_ranges.part_starting_offset_in_query;
            return total_offset_condition->checkInRange(
                1, part_offset_left.data(), part_offset_right.data(), part_offset_types, initial_mask);
        };

        BoolMask result(true, false);

        if (key_condition_useful)
            result = result & check_key_condition();

        if (part_offset_condition_useful)
            result = result & check_part_offset_condition();

        if (total_offset_condition_useful)
            result = result & check_total_offset_condition();

        return result;
    };

    bool key_condition_exact_range = !key_condition_useful || key_condition.matchesExactContinuousRange();
    bool part_offset_condition_exact_range = !part_offset_condition_useful || part_offset_condition->matchesExactContinuousRange();
    bool total_offset_condition_exact_range = !total_offset_condition_useful || total_offset_condition->matchesExactContinuousRange();
    const String & part_name = part->isProjectionPart() ? fmt::format("{}.{}", part->name, part->getParentPart()->name) : part->name;

    if (!key_condition_exact_range || !part_offset_condition_exact_range || !total_offset_condition_exact_range)
    {
        // Do exclusion search, where we drop ranges that do not match

        if (settings[Setting::merge_tree_coarse_index_granularity] <= 1)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Setting merge_tree_coarse_index_granularity should be greater than 1");

        size_t min_marks_for_seek = roundRowsOrBytesToMarks(
            settings[Setting::merge_tree_min_rows_for_seek],
            settings[Setting::merge_tree_min_bytes_for_seek],
            part->index_granularity_info.fixed_index_granularity,
            part->index_granularity_info.index_granularity_bytes);

        size_t steps = 0;

        for (const auto & part_range : part_with_ranges.ranges)
        {
            /// There will always be disjoint suspicious segments on the stack, the leftmost one at the top (back).
            /// At each step, take the left segment and check if it fits.
            /// If fits, split it into smaller ones and put them on the stack. If not, discard it.
            /// If the segment is already of one mark length, add it to response and discard it.

            std::vector<MarkRange> ranges_stack = {part_range};
            while (!ranges_stack.empty())
            {
                MarkRange range = ranges_stack.back();
                ranges_stack.pop_back();

                ++steps;

                auto result = check_in_range(
                    range, exact_ranges && range.end == range.begin + 1 ? BoolMask() : BoolMask::consider_only_can_be_true);
                if (!result.can_be_true)
                    continue;

                if (range.end == range.begin + 1)
                {
                    /// We saw a useful gap between neighboring marks. Either add it to the last range, or start a new range.
                    if (res.empty() || range.begin - res.back().end > min_marks_for_seek)
                        res.push_back(range);
                    else
                        res.back().end = range.end;

                    if (exact_ranges && !result.can_be_false)
                    {
                        if (exact_ranges->empty() || range.begin - exact_ranges->back().end > min_marks_for_seek)
                            exact_ranges->push_back(range);
                        else
                            exact_ranges->back().end = range.end;
                    }
                }
                else
                {
                    /// Break the segment and put the result on the stack from right to left.
                    size_t step = (range.end - range.begin - 1) / settings[Setting::merge_tree_coarse_index_granularity] + 1;
                    size_t end;

                    for (end = range.end; end > range.begin + step; end -= step)
                        ranges_stack.emplace_back(end - step, end);

                    ranges_stack.emplace_back(range.begin, end);
                }
            }
        }

        res.search_algorithm = MarkRanges::SearchAlgorithm::GenericExclusionSearch;
        ProfileEvents::increment(ProfileEvents::IndexGenericExclusionSearchAlgorithm);
        LOG_TRACE(
            log,
            "Used generic exclusion search {}over index for part {} with {} steps",
            exact_ranges ? "with exact ranges " : "",
            part_name,
            steps);
    }
    else
    {
        /// In case when SELECT's predicate defines a single continuous interval of keys,
        /// we can use binary search algorithm to find the left and right endpoint key marks of such interval.
        /// The returned value is the minimum range of marks, containing all keys for which KeyCondition holds

        res.search_algorithm = MarkRanges::SearchAlgorithm::BinarySearch;
        ProfileEvents::increment(ProfileEvents::IndexBinarySearchAlgorithm);
        LOG_TRACE(log, "Running binary search on index range for part {} ({} marks)", part_name, marks_count);

        size_t steps = 0;

        for (const auto & part_range : part_with_ranges.ranges)
        {
            MarkRange result_range;

            /// Invariant: !check_in_range(part_range.begin..searched_left).can_be_true
            ///             check_in_range(part_range.begin..searched_right).can_be_true
            /// (part_range.end + 1 is a sentinel out-of-bounds index, such that by definition
            ///  check_in_range(x..part_range.end+1).can_be_true is true. The binary search will never
            ///  actually call check_in_range with this out-of-bounds index.)
            /// If the condition is not true anywhere, we'll end up with searched_left == part_range.end.
            size_t searched_left = part_range.begin;
            size_t searched_right = part_range.end + 1;

            while (searched_left + 1 < searched_right)
            {
                const size_t middle = (searched_left + searched_right) / 2;
                MarkRange range(0, middle);
                if (check_in_range(range, BoolMask::consider_only_can_be_true).can_be_true)
                    searched_right = middle;
                else
                    searched_left = middle;
                ++steps;
            }
            result_range.begin = searched_left;
            LOG_TRACE(log, "Found (LEFT) boundary mark: {}", searched_left);

            /// Invariant:  check_in_range(searched_left..part_range.end).can_be_true
            ///            !check_in_range(searched_right..part_range.end).can_be_true
            /// (Except if searched_left was initially already equal to part_range.end.)
            searched_right = part_range.end;
            while (searched_left + 1 < searched_right)
            {
                const size_t middle = (searched_left + searched_right) / 2;
                MarkRange range(middle, part_range.end);
                if (check_in_range(range, BoolMask::consider_only_can_be_true).can_be_true)
                    searched_left = middle;
                else
                    searched_right = middle;
                ++steps;
            }
            result_range.end = searched_right;
            LOG_TRACE(log, "Found (RIGHT) boundary mark: {}", searched_right);

            if (result_range.begin < result_range.end)
            {
                res.emplace_back(result_range);

                if (exact_ranges)
                {
                    auto result_exact_range = result_range;
                    if (result_exact_range.begin < result_exact_range.end &&
                        check_in_range({result_exact_range.begin, result_exact_range.begin + 1}, BoolMask::consider_only_can_be_false).can_be_false)
                        ++result_exact_range.begin;

                    if (result_exact_range.begin < result_exact_range.end &&
                        check_in_range({result_exact_range.end - 1, result_exact_range.end}, BoolMask::consider_only_can_be_false).can_be_false)
                        --result_exact_range.end;

                    if (result_exact_range.begin < result_exact_range.end)
                    {
                        if (check_in_range(result_exact_range, BoolMask::consider_only_can_be_false).can_be_false)
                        {
                            /// key_condition.matchesExactContinuousRange returned true, but the
                            /// range doesn't seem to be continuous. Something's broken.
                            /// TODO: Remove the #ifndef and always throw after
                            ///       https://github.com/ClickHouse/ClickHouse/issues/90461 is fixed.
#ifndef NDEBUG
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent KeyCondition behavior");
#endif
                        }
                        else
                        {
                            exact_ranges->emplace_back(std::move(result_exact_range));
                        }
                    }
                }
            }
        }

        LOG_TRACE(
            log, "Found {} range {}in {} steps", res.empty() ? "empty" : "continuous", exact_ranges ? "with exact range " : "", steps);
    }

    return res;
}

std::pair<MarkRanges, RangesInDataPartReadHints> MergeTreeDataSelectExecutor::filterMarksUsingIndex(
    MergeTreeIndexPtr index_helper,
    MergeTreeIndexConditionPtr condition,
    const std::optional<KeyCondition> & key_condition_rpn_template,
    MergeTreeData::DataPartPtr part,
    const MarkRanges & ranges,
    const RangesInDataPartReadHints & in_read_hints,
    const MergeTreeReaderSettings & reader_settings,
    MarkCache * mark_cache,
    UncompressedCache * uncompressed_cache,
    VectorSimilarityIndexCache * vector_similarity_index_cache,
    bool use_skip_indexes_for_disjunctions,
    PartialDisjunctionResult & partial_disjunction_result,
    LoggerPtr log)
{
    if (!index_helper->getDeserializedFormat(part->checksums, index_helper->getFileName()))
    {
        LOG_DEBUG(log, "File for index {} does not exist ({}.*). Skipping it.", backQuote(index_helper->index.name),
            (fs::path(part->getDataPartStorage().getFullPath()) / index_helper->getFileName()).string());
        return {ranges, in_read_hints};
    }

    /// Whether we should use a more optimal filtering.
    bool bulk_filtering = reader_settings.secondary_indices_enable_bulk_filtering && index_helper->supportsBulkFiltering() && !use_skip_indexes_for_disjunctions;

    auto index_granularity = index_helper->index.granularity;

    const size_t min_marks_for_seek = roundRowsOrBytesToMarks(
        reader_settings.merge_tree_min_rows_for_seek,
        reader_settings.merge_tree_min_bytes_for_seek,
        part->index_granularity_info.fixed_index_granularity,
        part->index_granularity_info.index_granularity_bytes);

    size_t marks_count = part->index_granularity->getMarksCountWithoutFinal();
    size_t index_marks_count = (marks_count + index_granularity - 1) / index_granularity;

    /// The vector similarity index can only be used if the PK did not prune some ranges within the part.
    /// (the vector index is built on the entire part).
    const bool all_match  = (marks_count == ranges.getNumberOfMarks());
    if (index_helper->isVectorSimilarityIndex() && !all_match)
    {
        return {ranges, in_read_hints};
    }

    MarkRanges index_ranges;
    for (const auto & range : ranges)
    {
        MarkRange index_range(
                range.begin / index_granularity,
                (range.end + index_granularity - 1) / index_granularity);
        index_ranges.push_back(index_range);
    }

    MergeTreeIndexReader reader(
        index_helper, part,
        index_marks_count,
        index_ranges,
        mark_cache,
        uncompressed_cache,
        vector_similarity_index_cache,
        reader_settings);

    MarkRanges res;
    size_t ranges_size = ranges.size();
    RangesInDataPartReadHints read_hints = in_read_hints;

    auto create_update_partial_disjunction_result_fn = [&](size_t range_begin) -> KeyCondition::UpdatePartialDisjunctionResultFn
    {
        if (use_skip_indexes_for_disjunctions && key_condition_rpn_template)
        {
            return [range_begin, &partial_disjunction_result](size_t position, bool element_result, bool is_unknown)
            {
                if (!is_unknown)
                    partial_disjunction_result[(range_begin * MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT) + position] = element_result;
            };
        }
        return nullptr;
    };

    if (index_helper->isTextIndex())
    {
        MergeTreeIndexGranulePtr granule;
        reader.read(0, condition.get(), granule);
        auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule);

        for (const auto & range : ranges)
        {
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                size_t row_begin = part->index_granularity->getMarkStartingRow(mark);
                size_t row_end = part->index_granularity->getMarkStartingRow(mark + 1);

                if (row_begin == row_end)
                    continue;

                granule_text.setCurrentRange(RowsRange(row_begin, row_end - 1));
                bool may_be_true = condition->mayBeTrueOnGranule(granule, create_update_partial_disjunction_result_fn(mark));

                if (may_be_true)
                {
                    if (res.empty() || mark - res.back().end > min_marks_for_seek)
                        res.push_back(MarkRange(mark, mark + 1));
                    else
                        res.back().end = mark + 1;
                }
            }
        }
    }
    else if (bulk_filtering)
    {
        MergeTreeIndexBulkGranulesPtr granules;
        size_t current_granule_num = 0;

        for (size_t i = 0; i < ranges_size; ++i)
        {
            const MarkRange & index_range = index_ranges[i];

            for (size_t index_mark = index_range.begin; index_mark < index_range.end; ++index_mark)
            {
                reader.read(index_mark, current_granule_num, granules);
                ++current_granule_num;
            }
        }

        IMergeTreeIndexCondition::FilteredGranules filtered_granules = condition->getPossibleGranules(granules);
        if (filtered_granules.empty())
            return {res, read_hints};

        auto it = filtered_granules.begin();
        current_granule_num = 0;
        for (size_t i = 0; i < ranges_size; ++i)
        {
            const MarkRange & index_range = index_ranges[i];

            for (size_t index_mark = index_range.begin; index_mark < index_range.end; ++index_mark)
            {
                if (current_granule_num == *it)
                {
                    MarkRange data_range(
                        std::max(ranges[i].begin, index_mark * index_granularity),
                        std::min(ranges[i].end, (index_mark + 1) * index_granularity));

                    if (res.empty() || data_range.begin - res.back().end > min_marks_for_seek)
                        res.push_back(data_range);
                    else
                        res.back().end = data_range.end;

                    ++it;
                    if (it == filtered_granules.end())
                        break;
                }

                ++current_granule_num;
            }

            if (it == filtered_granules.end())
                break;
        }
    }
    else
    {
        /// Some granules can cover two or more ranges,
        /// this variable is stored to avoid reading the same granule twice.
        MergeTreeIndexGranulePtr granule = nullptr;
        size_t last_index_mark = 0;

        for (size_t i = 0; i < ranges_size; ++i)
        {
            const MarkRange & index_range = index_ranges[i];

            for (size_t index_mark = index_range.begin; index_mark < index_range.end; ++index_mark)
            {
                if (index_mark != index_range.begin || !granule || last_index_mark != index_range.begin)
                {
                    reader.read(index_mark, condition.get(), granule);
                }

                if (index_helper->isVectorSimilarityIndex())
                {
                    read_hints.vector_search_results = condition->calculateApproximateNearestNeighbors(granule);

                    /// We need to sort the result ranges ascendingly
                    auto rows = read_hints.vector_search_results.value().rows;
                    std::sort(rows.begin(), rows.end());
#ifndef NDEBUG
                    /// Duplicates should in theory not be possible but better be safe than sorry ...
                    const bool has_duplicates = std::adjacent_find(rows.begin(), rows.end()) != rows.end();
                    if (has_duplicates)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Usearch returned duplicate row numbers");
#endif
                    if (!(read_hints.vector_search_results.value().distances.has_value()))
                        read_hints = {};

                    for (auto row : rows)
                    {
                        size_t num_marks = part->index_granularity->countMarksForRows(index_mark * index_granularity, row);

                        MarkRange data_range(
                            std::max(ranges[i].begin, (index_mark * index_granularity) + num_marks),
                            std::min(ranges[i].end, (index_mark * index_granularity) + num_marks + 1));

                        if (!res.empty() && data_range.end == res.back().end)
                            /// Vector search may return >1 hit within the same granule/mark. Don't add to the result twice.
                            continue;

                        if (res.empty() || data_range.begin - res.back().end > min_marks_for_seek)
                            res.push_back(data_range);
                        else
                            res.back().end = data_range.end;
                    }
                }
                else
                {
                    size_t range_begin = std::max(ranges[i].begin, index_mark * index_granularity);
                    bool may_be_true = condition->mayBeTrueOnGranule(granule, create_update_partial_disjunction_result_fn(range_begin));

                    if (!may_be_true)
                        continue;

                    MarkRange data_range(
                        std::max(ranges[i].begin, index_mark * index_granularity),
                        std::min(ranges[i].end, (index_mark + 1) * index_granularity));

                    if (res.empty() || data_range.begin - res.back().end > min_marks_for_seek)
                        res.push_back(data_range);
                    else
                        res.back().end = data_range.end;
                }
            }

            last_index_mark = index_range.end - 1;
        }
    }

    return {res, read_hints};
}

MarkRanges MergeTreeDataSelectExecutor::filterMarksUsingMergedIndex(
    MergeTreeIndices indices,
    MergeTreeIndexMergedConditionPtr condition,
    MergeTreeData::DataPartPtr part,
    const MarkRanges & ranges,
    const MergeTreeReaderSettings & reader_settings,
    MarkCache * mark_cache,
    UncompressedCache * uncompressed_cache,
    VectorSimilarityIndexCache * vector_similarity_index_cache,
    LoggerPtr log)
{
    for (const auto & index_helper : indices)
    {
        /// Check for both original and hashed filenames (hashed if the index name is too long)
        auto index_file_name = IMergeTreeDataPart::getStreamNameOrHash(
            index_helper->getFileName(), ".idx", part->getDataPartStorage());
        if (!index_file_name)
        {
            LOG_DEBUG(log, "File for index {} does not exist. Skipping it.", backQuote(index_helper->index.name));
            return ranges;
        }
    }

    auto index_granularity = indices.front()->index.granularity;

    const size_t min_marks_for_seek = roundRowsOrBytesToMarks(
        reader_settings.merge_tree_min_rows_for_seek,
        reader_settings.merge_tree_min_bytes_for_seek,
        part->index_granularity_info.fixed_index_granularity,
        part->index_granularity_info.index_granularity_bytes);

    size_t marks_count = part->index_granularity->getMarksCountWithoutFinal();
    size_t index_marks_count = (marks_count + index_granularity - 1) / index_granularity;

    MarkRanges index_ranges;
    for (const auto & range : ranges)
    {
        MarkRange index_range(
                range.begin / index_granularity,
                (range.end + index_granularity - 1) / index_granularity);
        index_ranges.push_back(index_range);
    }

    std::vector<std::unique_ptr<MergeTreeIndexReader>> readers;
    for (const auto & index_helper : indices)
    {
        readers.emplace_back(
            std::make_unique<MergeTreeIndexReader>(
                index_helper,
                part,
                index_marks_count,
                index_ranges,
                mark_cache,
                uncompressed_cache,
                vector_similarity_index_cache,
                reader_settings));
    }

    MarkRanges res;

    /// Some granules can cover two or more ranges,
    /// this variable is stored to avoid reading the same granule twice.
    MergeTreeIndexGranules granules(indices.size(), nullptr);
    bool granules_filled = false;
    size_t last_index_mark = 0;
    for (const auto & range : ranges)
    {
        MarkRange index_range(
            range.begin / index_granularity,
            (range.end + index_granularity - 1) / index_granularity);

        for (size_t index_mark = index_range.begin; index_mark < index_range.end; ++index_mark)
        {
            if (index_mark != index_range.begin || !granules_filled || last_index_mark != index_range.begin)
            {
                for (size_t i = 0; i < readers.size(); ++i)
                {
                    readers[i]->read(index_mark, nullptr, granules[i]);
                    granules_filled = true;
                }
            }

            if (!condition->mayBeTrueOnGranule(granules))
                continue;

            MarkRange data_range(
                std::max(range.begin, index_mark * index_granularity),
                std::min(range.end, (index_mark + 1) * index_granularity));

            if (res.empty() || data_range.begin - res.back().end > min_marks_for_seek)
                res.push_back(data_range);
            else
                res.back().end = data_range.end;
        }

        last_index_mark = index_range.end - 1;
    }

    return res;
}

RangesInDataParts MergeTreeDataSelectExecutor::selectPartsToRead(
    const RangesInDataParts & parts,
    const std::optional<std::unordered_set<String>> & part_values,
    const std::optional<KeyCondition> & minmax_idx_condition,
    const DataTypes & minmax_columns_types,
    const std::optional<PartitionPruner> & partition_pruner,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    PartFilterCounters & counters,
    QueryStatusPtr query_status)
{
    RangesInDataParts res_parts;

    for (const auto & prev_part : parts)
    {
        const auto & part_or_projection = prev_part.data_part;

        if (query_status)
            query_status->checkTimeLimit();

        const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
        if (part_values && !part_values->contains(part->name))
            continue;

        if (part->isEmpty())
            continue;

        if (max_block_numbers_to_read)
        {
            auto blocks_iterator = max_block_numbers_to_read->find(part->info.getPartitionId());
            if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                continue;
        }

        size_t num_granules = part->index_granularity->getMarksCountWithoutFinal();

        counters.num_initial_selected_parts += 1;
        counters.num_initial_selected_granules += num_granules;

        if (minmax_idx_condition && !minmax_idx_condition->checkInHyperrectangle(
                part->minmax_idx->hyperrectangle, minmax_columns_types).can_be_true)
            continue;

        counters.num_parts_after_minmax += 1;
        counters.num_granules_after_minmax += num_granules;

        if (partition_pruner)
        {
            if (partition_pruner->canBePruned(*part))
                continue;
        }

        counters.num_parts_after_partition_pruner += 1;
        counters.num_granules_after_partition_pruner += num_granules;

        res_parts.push_back(prev_part);
    }
    return res_parts;
}

RangesInDataParts MergeTreeDataSelectExecutor::selectPartsToReadWithUUIDFilter(
    const RangesInDataParts & parts,
    const std::optional<std::unordered_set<String>> & part_values,
    MergeTreeData::PinnedPartUUIDsPtr pinned_part_uuids,
    const std::optional<KeyCondition> & minmax_idx_condition,
    const DataTypes & minmax_columns_types,
    const std::optional<PartitionPruner> & partition_pruner,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    ContextPtr query_context,
    PartFilterCounters & counters,
    LoggerPtr log)
{
    /// process_parts prepare parts that have to be read for the query,
    /// returns false if duplicated parts' UUID have been met
    auto select_parts = [&](const RangesInDataParts & in_parts, RangesInDataParts & selected_parts) -> bool
    {
        auto ignored_part_uuids = query_context->getIgnoredPartUUIDs();
        std::unordered_set<UUID> temp_part_uuids;

        for (const auto & prev_part : in_parts)
        {
            const auto & part_or_projection = prev_part.data_part;
            const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
            if (part_values && !part_values->contains(part->name))
                continue;

            if (part->isEmpty())
                continue;

            if (max_block_numbers_to_read)
            {
                auto blocks_iterator = max_block_numbers_to_read->find(part->info.getPartitionId());
                if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                    continue;
            }

            /// Skip the part if its uuid is meant to be excluded
            if (part->uuid != UUIDHelpers::Nil && ignored_part_uuids->has(part->uuid))
                continue;

            size_t num_granules = part->index_granularity->getMarksCountWithoutFinal();

            counters.num_initial_selected_parts += 1;
            counters.num_initial_selected_granules += num_granules;

            if (minmax_idx_condition
                && !minmax_idx_condition->checkInHyperrectangle(part->minmax_idx->hyperrectangle, minmax_columns_types)
                        .can_be_true)
                continue;

            counters.num_parts_after_minmax += 1;
            counters.num_granules_after_minmax += num_granules;

            if (partition_pruner)
            {
                if (partition_pruner->canBePruned(*part))
                    continue;
            }

            counters.num_parts_after_partition_pruner += 1;
            counters.num_granules_after_partition_pruner += num_granules;

            /// populate UUIDs and exclude ignored parts if enabled
            if (part->uuid != UUIDHelpers::Nil && pinned_part_uuids->contains(part->uuid))
            {
                auto result = temp_part_uuids.insert(part->uuid);
                if (!result.second)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Found a part with the same UUID on the same replica.");
            }

            selected_parts.push_back(prev_part);
        }

        if (!temp_part_uuids.empty())
        {
            auto duplicates = query_context->getPartUUIDs()->add(std::vector<UUID>{temp_part_uuids.begin(), temp_part_uuids.end()});
            if (!duplicates.empty())
            {
                /// on a local replica with prefer_localhost_replica=1 if any duplicates appeared during the first pass,
                /// adding them to the exclusion, so they will be skipped on second pass
                query_context->getIgnoredPartUUIDs()->add(duplicates);
                return false;
            }
        }

        return true;
    };

    /// Process parts that have to be read for a query.
    RangesInDataParts filtered_parts = {};
    auto needs_retry = !select_parts(parts, filtered_parts);

    /// If any duplicated part UUIDs met during the first step, try to ignore them in second pass.
    /// This may happen when `prefer_localhost_replica` is set and "distributed" stage runs in the same process with "remote" stage.
    if (needs_retry)
    {
        LOG_DEBUG(log, "Found duplicate uuids locally, will retry part selection without them");

        counters = PartFilterCounters();

        auto initial_filtered_parts = filtered_parts;
        filtered_parts = {};

        /// Second attempt didn't help, throw an exception
        if (!select_parts(initial_filtered_parts, filtered_parts))
            throw Exception(ErrorCodes::DUPLICATED_PART_UUIDS, "Found duplicate UUIDs while processing query.");
    }

    return filtered_parts;
}

/// Read and return index granules from a minmax index.
MergeTreeIndexBulkGranulesMinMaxPtr MergeTreeDataSelectExecutor::getMinMaxIndexGranules(
    MergeTreeData::DataPartPtr part,
    MergeTreeIndexPtr skip_index_minmax,
    const MarkRanges & ranges,
    int direction,
    bool access_by_mark,
    const MergeTreeReaderSettings & reader_settings,
    MarkCache * mark_cache,
    UncompressedCache * uncompressed_cache,
    VectorSimilarityIndexCache * vector_similarity_index_cache)
{
    if (!skip_index_minmax->getDeserializedFormat(part->checksums, skip_index_minmax->getFileName()))
    {
        return nullptr;
    }

    auto index_granularity = skip_index_minmax->index.granularity;
    size_t marks_count = part->index_granularity->getMarksCountWithoutFinal();
    size_t index_marks_count = (marks_count + index_granularity - 1) / index_granularity;

    MarkRanges index_ranges;
    for (const auto & range : ranges)
    {
        MarkRange index_range(
            range.begin / index_granularity,
            (range.end + index_granularity - 1) / index_granularity);
        index_ranges.push_back(index_range);
    }

    MergeTreeIndexReader reader(
            skip_index_minmax,
            part,
            index_marks_count,
            index_ranges,
            mark_cache,
            uncompressed_cache,
            vector_similarity_index_cache,
            reader_settings);

    auto min_max_granules = std::make_shared<MergeTreeIndexBulkGranulesMinMax>(skip_index_minmax->index.name,
                                    skip_index_minmax->index.sample_block, direction, index_ranges.getNumberOfMarks(), access_by_mark);
    auto bulk_granules = std::dynamic_pointer_cast<IMergeTreeIndexBulkGranules>(min_max_granules);

    for (auto index_range : index_ranges)
    {
        for (size_t index_mark = index_range.begin; index_mark < index_range.end; ++index_mark)
        {
            reader.read(index_mark, index_mark, bulk_granules);
        }
    }
    return min_max_granules;
}

/// Evaluate the predicate condition using the saved partial results.
/// e.g (A = 5 OR (B = 5 AND C = 5))
/// For each range, we have saved the results of evaluation with
/// skip index A, skip index B, skip index C. Now we run a "final pass"
/// to see if the range qualifies on the whole condition.
/// rpn_template_for_eval_result is a "template" only. Hence the code
/// below only processes 5 specific RPNElement types.
MarkRanges MergeTreeDataSelectExecutor::mergePartialResultsForDisjunctions(
    MergeTreeData::DataPartPtr part,
    const MarkRanges & ranges,
    const KeyCondition & rpn_template_for_eval_result,
    const PartialDisjunctionResult & partial_eval_results,
    MergeTreeReaderSettings reader_settings,
    LoggerPtr log)
{
    MarkRanges res;

    auto rpn_template_for_eval_result_string = rpn_template_for_eval_result.toString();

    LOG_DEBUG(log, "Entered mergePartialResultsForDisjunctions for part {}, rpn = {}",
              part->name, rpn_template_for_eval_result_string);

    const size_t min_marks_for_seek = roundRowsOrBytesToMarks(
                reader_settings.merge_tree_min_rows_for_seek,
                reader_settings.merge_tree_min_bytes_for_seek,
                part->index_granularity_info.fixed_index_granularity,
                part->index_granularity_info.index_granularity_bytes);

    /// Evaluate the RPN over all the ranges.
    auto rpn = rpn_template_for_eval_result.getRPN();
    std::vector<bool> rpn_stack;
    for (auto range : ranges)
    {
        for (auto range_begin = range.begin; range_begin < range.end; range_begin++)
        {
            rpn_stack.clear();
            size_t position = 0;
            for (const auto & element : rpn)
            {
                if (element.function == KeyCondition::RPNElement::FUNCTION_UNKNOWN)
                {
                    rpn_stack.emplace_back(partial_eval_results[(range_begin * MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT) + position]);
                }
                else if (element.function == KeyCondition::RPNElement::FUNCTION_NOT)
                {
                    /// NOT x = 'acd' is always True with BloomFilter even if x = 'acd' was True.
                    /// Hence rpn_stack.back() = !rpn_stack.back() will be wrong. We will
                    /// push True by default for NOT. This could cause false positives for
                    /// other skip indexes if NOT is used.
                    rpn_stack.back() = true;
                }
                else if (element.function == KeyCondition::RPNElement::FUNCTION_OR)
                {
                    auto arg1 = rpn_stack.back();
                    rpn_stack.pop_back();
                    auto arg2 = rpn_stack.back();
                    rpn_stack.back() = arg1 || arg2;
                }
                else if (element.function == KeyCondition::RPNElement::FUNCTION_AND)
                {
                    auto arg1 = rpn_stack.back();
                    rpn_stack.pop_back();
                    auto arg2 = rpn_stack.back();
                    rpn_stack.back() = arg1 && arg2;
                }
                else if (element.function == KeyCondition::RPNElement::ALWAYS_TRUE)
                {
                    rpn_stack.emplace_back(true);
                }
                else if (element.function == KeyCondition::RPNElement::ALWAYS_FALSE)
                {
                    rpn_stack.emplace_back(false);
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type {} in mergePartialResultsForDisjunctions(), RPN = {}", element.function, rpn_template_for_eval_result_string);

                position++;
            }

            if (rpn_stack.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size {} in mergePartialResultsForDisjunctions(), RPN = {}", rpn_stack.size(), rpn_template_for_eval_result_string);

            if (rpn_stack[0])
            {
                if (res.empty() || range_begin - res.back().end > min_marks_for_seek)
                    res.push_back({range_begin, range_begin + 1});
                else
                    res.back().end = range_begin + 1;
            }
        }
    }
    return res;
}

}
