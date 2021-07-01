#include <boost/rational.hpp>   /// For calculations related to sampling coefficients.
#include <common/scope_guard_safe.h>
#include <optional>
#include <unordered_set>

#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Context.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <Core/UUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/VirtualColumnUtils.h>

#include <Interpreters/InterpreterSelectQuery.h>

#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <IO/WriteBufferFromOStream.h>

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INDEX_NOT_USED;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int ILLEGAL_COLUMN;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_MANY_ROWS;
    extern const int CANNOT_PARSE_TEXT;
    extern const int TOO_MANY_PARTITIONS;
    extern const int DUPLICATED_PART_UUIDS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int PROJECTION_NOT_USED;
}


MergeTreeDataSelectExecutor::MergeTreeDataSelectExecutor(const MergeTreeData & data_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + " (SelectExecutor)"))
{
}

size_t MergeTreeDataSelectExecutor::getApproximateTotalRowsToRead(
    const MergeTreeData::DataPartsVector & parts,
    const StorageMetadataPtr & metadata_snapshot,
    const KeyCondition & key_condition,
    const Settings & settings,
    Poco::Logger * log)
{
    size_t rows_count = 0;

    /// We will find out how many rows we would have read without sampling.
    LOG_DEBUG(log, "Preliminary index scan with condition: {}", key_condition.toString());

    for (const auto & part : parts)
    {
        MarkRanges ranges = markRangesFromPKRange(part, metadata_snapshot, key_condition, settings, log);

        /** In order to get a lower bound on the number of rows that match the condition on PK,
          *  consider only guaranteed full marks.
          * That is, do not take into account the first and last marks, which may be incomplete.
          */
        for (const auto & range : ranges)
            if (range.end - range.begin > 2)
                rows_count += part->index_granularity.getRowsCountInRange({range.begin + 1, range.end - 1});

    }

    return rows_count;
}


using RelativeSize = boost::rational<ASTSampleRatio::BigNum>;

static std::string toString(const RelativeSize & x)
{
    return ASTSampleRatio::toString(x.numerator()) + "/" + ASTSampleRatio::toString(x.denominator());
}

/// Converts sample size to an approximate number of rows (ex. `SAMPLE 1000000`) to relative value (ex. `SAMPLE 0.1`).
static RelativeSize convertAbsoluteSampleSizeToRelative(const ASTPtr & node, size_t approx_total_rows)
{
    if (approx_total_rows == 0)
        return 1;

    const auto & node_sample = node->as<ASTSampleRatio &>();

    auto absolute_sample_size = node_sample.ratio.numerator / node_sample.ratio.denominator;
    return std::min(RelativeSize(1), RelativeSize(absolute_sample_size) / RelativeSize(approx_total_rows));
}


QueryPlanPtr MergeTreeDataSelectExecutor::read(
    const Names & column_names_to_return,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const UInt64 max_block_size,
    const unsigned num_streams,
    QueryProcessingStage::Enum processed_stage,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read) const
{
    const auto & settings = context->getSettingsRef();
    auto parts = data.getDataPartsVector();
    if (!query_info.projection)
    {
        auto plan = readFromParts(
            parts,
            column_names_to_return,
            metadata_snapshot,
            metadata_snapshot,
            query_info,
            context,
            max_block_size,
            num_streams,
            max_block_numbers_to_read);

        if (plan->isInitialized() && settings.allow_experimental_projection_optimization && settings.force_optimize_projection
            && !metadata_snapshot->projections.empty())
            throw Exception(
                "No projection is used when allow_experimental_projection_optimization = 1 and force_optimize_projection = 1",
                ErrorCodes::PROJECTION_NOT_USED);

        return plan;
    }

    LOG_DEBUG(
        log,
        "Choose {} projection {}",
        ProjectionDescription::typeToString(query_info.projection->desc->type),
        query_info.projection->desc->name);

    MergeTreeData::DataPartsVector projection_parts;
    MergeTreeData::DataPartsVector normal_parts;
    for (const auto & part : parts)
    {
        const auto & projections = part->getProjectionParts();
        auto it = projections.find(query_info.projection->desc->name);
        if (it != projections.end())
            projection_parts.push_back(it->second);
        else
            normal_parts.push_back(part);
    }

    Pipes pipes;
    Pipe projection_pipe;
    Pipe ordinary_pipe;

    const auto & given_select = query_info.query->as<const ASTSelectQuery &>();
    if (!projection_parts.empty())
    {
        LOG_DEBUG(log, "projection required columns: {}", fmt::join(query_info.projection->required_columns, ", "));
        auto plan = readFromParts(
            projection_parts,
            query_info.projection->required_columns,
            metadata_snapshot,
            query_info.projection->desc->metadata,
            query_info,
            context,
            max_block_size,
            num_streams,
            max_block_numbers_to_read);

        if (plan)
        {
            // If `before_where` is not empty, transform input blocks by adding needed columns
            // originated from key columns. We already project the block at the end, using
            // projection_block, so we can just add more columns here without worrying
            // NOTE: prewhere is executed inside readFromParts
            if (query_info.projection->before_where)
            {
                // std::cerr << fmt::format("projection before_where: {}", query_info.projection->before_where->dumpDAG());
                auto where_step = std::make_unique<FilterStep>(
                    plan->getCurrentDataStream(),
                    query_info.projection->before_where,
                    query_info.projection->where_column_name,
                    query_info.projection->remove_where_filter);

                where_step->setStepDescription("WHERE");
                plan->addStep(std::move(where_step));
            }

            if (query_info.projection->before_aggregation)
            {
                // std::cerr << fmt::format("projection before_aggregation: {}", query_info.projection->before_aggregation->dumpDAG());
                auto expression_before_aggregation
                    = std::make_unique<ExpressionStep>(plan->getCurrentDataStream(), query_info.projection->before_aggregation);
                expression_before_aggregation->setStepDescription("Before GROUP BY");
                plan->addStep(std::move(expression_before_aggregation));
            }
            projection_pipe = plan->convertToPipe(
                QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
        }
    }

    if (!normal_parts.empty())
    {
        auto storage_from_base_parts_of_projection = StorageFromMergeTreeDataPart::create(std::move(normal_parts));
        auto ast = query_info.projection->desc->query_ast->clone();
        auto & select = ast->as<ASTSelectQuery &>();
        if (given_select.where())
            select.setExpression(ASTSelectQuery::Expression::WHERE, given_select.where()->clone());
        if (given_select.prewhere())
            select.setExpression(ASTSelectQuery::Expression::WHERE, given_select.prewhere()->clone());

        // After overriding the group by clause, we finish the possible aggregations directly
        if (processed_stage >= QueryProcessingStage::Enum::WithMergeableState && given_select.groupBy())
            select.setExpression(ASTSelectQuery::Expression::GROUP_BY, given_select.groupBy()->clone());
        auto interpreter = InterpreterSelectQuery(
            ast,
            context,
            storage_from_base_parts_of_projection,
            nullptr,
            SelectQueryOptions{processed_stage}.ignoreAggregation().ignoreProjections());
        ordinary_pipe = QueryPipeline::getPipe(interpreter.execute().pipeline);
    }

    if (query_info.projection->desc->type == ProjectionDescription::Type::Aggregate)
    {
        /// Here we create shared ManyAggregatedData for both projection and ordinary data.
        /// For ordinary data, AggregatedData is filled in a usual way.
        /// For projection data, AggregatedData is filled by merging aggregation states.
        /// When all AggregatedData is filled, we merge aggregation states together in a usual way.
        /// Pipeline will look like:
        /// ReadFromProjection   -> Aggregating (only merge states) ->
        /// ReadFromProjection   -> Aggregating (only merge states) ->
        /// ...                                                     -> Resize -> ConvertingAggregatedToChunks
        /// ReadFromOrdinaryPart -> Aggregating (usual)             ->           (added by last Aggregating)
        /// ReadFromOrdinaryPart -> Aggregating (usual)             ->
        /// ...
        auto many_data = std::make_shared<ManyAggregatedData>(projection_pipe.numOutputPorts() + ordinary_pipe.numOutputPorts());
        size_t counter = 0;

        // TODO apply in_order_optimization here
        auto build_aggregate_pipe = [&](Pipe & pipe, bool projection)
        {
            const auto & header_before_aggregation = pipe.getHeader();

            // std::cerr << "============ header before aggregation" << std::endl;
            // std::cerr << header_before_aggregation.dumpStructure() << std::endl;

            ColumnNumbers keys;
            for (const auto & key : query_info.projection->aggregation_keys)
                keys.push_back(header_before_aggregation.getPositionByName(key.name));

            AggregateDescriptions aggregates = query_info.projection->aggregate_descriptions;
            if (!projection)
            {
                for (auto & descr : aggregates)
                    if (descr.arguments.empty())
                        for (const auto & name : descr.argument_names)
                            descr.arguments.push_back(header_before_aggregation.getPositionByName(name));
            }

            AggregatingTransformParamsPtr transform_params;
            if (projection)
            {
                Aggregator::Params params(
                    header_before_aggregation,
                    keys,
                    aggregates,
                    query_info.projection->aggregate_overflow_row,
                    settings.max_rows_to_group_by,
                    settings.group_by_overflow_mode,
                    settings.group_by_two_level_threshold,
                    settings.group_by_two_level_threshold_bytes,
                    settings.max_bytes_before_external_group_by,
                    settings.empty_result_for_aggregation_by_empty_set,
                    context->getTemporaryVolume(),
                    settings.max_threads,
                    settings.min_free_disk_space_for_temporary_data,
                    settings.compile_expressions,
                    settings.min_count_to_compile_aggregate_expression,
                    header_before_aggregation); // The source header is also an intermediate header

                transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), query_info.projection->aggregate_final);

                /// This part is hacky.
                /// We want AggregatingTransform to work with aggregate states instead of normal columns.
                /// It is almost the same, just instead of adding new data to aggregation state we merge it with existing.
                ///
                /// It is needed because data in projection:
                /// * is not merged completely (we may have states with the same key in different parts)
                /// * is not split into buckets (so if we just use MergingAggregated, it will use single thread)
                transform_params->only_merge = true;
            }
            else
            {
                Aggregator::Params params(
                    header_before_aggregation,
                    keys,
                    aggregates,
                    query_info.projection->aggregate_overflow_row,
                    settings.max_rows_to_group_by,
                    settings.group_by_overflow_mode,
                    settings.group_by_two_level_threshold,
                    settings.group_by_two_level_threshold_bytes,
                    settings.max_bytes_before_external_group_by,
                    settings.empty_result_for_aggregation_by_empty_set,
                    context->getTemporaryVolume(),
                    settings.max_threads,
                    settings.min_free_disk_space_for_temporary_data,
                    settings.compile_aggregate_expressions,
                    settings.min_count_to_compile_aggregate_expression);

                transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), query_info.projection->aggregate_final);
            }

            pipe.resize(pipe.numOutputPorts(), true, true);

            auto merge_threads = num_streams;
            auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
                ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                : static_cast<size_t>(settings.max_threads);

            pipe.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<AggregatingTransform>(
                    header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
            });

            // std::cerr << "============ header after aggregation" << std::endl;
            // std::cerr << pipe.getHeader().dumpStructure() << std::endl;
        };

        if (!projection_pipe.empty())
            build_aggregate_pipe(projection_pipe, true);
        if (!ordinary_pipe.empty())
            build_aggregate_pipe(ordinary_pipe, false);
    }

    pipes.emplace_back(std::move(projection_pipe));
    pipes.emplace_back(std::move(ordinary_pipe));
    auto pipe = Pipe::unitePipes(std::move(pipes));
    // TODO what if pipe is empty?
    pipe.resize(1);

    auto step = std::make_unique<ReadFromStorageStep>(std::move(pipe), "MergeTree(with projection)");
    auto plan = std::make_unique<QueryPlan>();
    plan->addStep(std::move(step));
    return plan;
}

MergeTreeDataSelectSamplingData MergeTreeDataSelectExecutor::getSampling(
    const ASTSelectQuery & select,
    NamesAndTypesList available_real_columns,
    const MergeTreeData::DataPartsVector & parts,
    KeyCondition & key_condition,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    bool sample_factor_column_queried,
    Poco::Logger * log)
{
    const Settings & settings = context->getSettingsRef();
    /// Sampling.
    MergeTreeDataSelectSamplingData sampling;

    RelativeSize relative_sample_size = 0;
    RelativeSize relative_sample_offset = 0;

    auto select_sample_size = select.sampleSize();
    auto select_sample_offset = select.sampleOffset();

    if (select_sample_size)
    {
        relative_sample_size.assign(
            select_sample_size->as<ASTSampleRatio &>().ratio.numerator,
            select_sample_size->as<ASTSampleRatio &>().ratio.denominator);

        if (relative_sample_size < 0)
            throw Exception("Negative sample size", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        relative_sample_offset = 0;
        if (select_sample_offset)
            relative_sample_offset.assign(
                select_sample_offset->as<ASTSampleRatio &>().ratio.numerator,
                select_sample_offset->as<ASTSampleRatio &>().ratio.denominator);

        if (relative_sample_offset < 0)
            throw Exception("Negative sample offset", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        /// Convert absolute value of the sampling (in form `SAMPLE 1000000` - how many rows to
        /// read) into the relative `SAMPLE 0.1` (how much data to read).
        size_t approx_total_rows = 0;
        if (relative_sample_size > 1 || relative_sample_offset > 1)
            approx_total_rows = getApproximateTotalRowsToRead(parts, metadata_snapshot, key_condition, settings, log);

        if (relative_sample_size > 1)
        {
            relative_sample_size = convertAbsoluteSampleSizeToRelative(select_sample_size, approx_total_rows);
            LOG_DEBUG(log, "Selected relative sample size: {}", toString(relative_sample_size));
        }

        /// SAMPLE 1 is the same as the absence of SAMPLE.
        if (relative_sample_size == RelativeSize(1))
            relative_sample_size = 0;

        if (relative_sample_offset > 0 && RelativeSize(0) == relative_sample_size)
            throw Exception("Sampling offset is incorrect because no sampling", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (relative_sample_offset > 1)
        {
            relative_sample_offset = convertAbsoluteSampleSizeToRelative(select_sample_offset, approx_total_rows);
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

    /// Parallel replicas has been requested but there is no way to sample data.
    /// Select all data from first replica and no data from other replicas.
    if (settings.parallel_replicas_count > 1 && !data.supportsSampling() && settings.parallel_replica_offset > 0)
    {
        LOG_DEBUG(log, "Will use no data on this replica because parallel replicas processing has been requested"
            " (the setting 'max_parallel_replicas') but the table does not support sampling and this replica is not the first.");
        sampling.read_nothing = true;
        return sampling;
    }

    sampling.use_sampling = relative_sample_size > 0 || (settings.parallel_replicas_count > 1 && data.supportsSampling());
    bool no_data = false;   /// There is nothing left after sampling.

    if (sampling.use_sampling)
    {
        if (sample_factor_column_queried && relative_sample_size != RelativeSize(0))
            sampling.used_sample_factor = 1.0 / boost::rational_cast<Float64>(relative_sample_size);

        RelativeSize size_of_universum = 0;
        const auto & sampling_key = metadata_snapshot->getSamplingKey();
        DataTypePtr sampling_column_type = sampling_key.data_types[0];

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
            throw Exception(
                "Invalid sampling column type in storage parameters: " + sampling_column_type->getName()
                    + ". Must be one unsigned integer type",
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

        if (settings.parallel_replicas_count > 1)
        {
            if (relative_sample_size == RelativeSize(0))
                relative_sample_size = 1;

            relative_sample_size /= settings.parallel_replicas_count.value;
            relative_sample_offset += relative_sample_size * RelativeSize(settings.parallel_replica_offset.value);
        }

        if (relative_sample_offset >= RelativeSize(1))
            no_data = true;

        /// Calculate the half-interval of `[lower, upper)` column values.
        bool has_lower_limit = false;
        bool has_upper_limit = false;

        RelativeSize lower_limit_rational = relative_sample_offset * size_of_universum;
        RelativeSize upper_limit_rational = (relative_sample_offset + relative_sample_size) * size_of_universum;

        UInt64 lower = boost::rational_cast<ASTSampleRatio::BigNum>(lower_limit_rational);
        UInt64 upper = boost::rational_cast<ASTSampleRatio::BigNum>(upper_limit_rational);

        if (lower > 0)
            has_lower_limit = true;

        if (upper_limit_rational < size_of_universum)
            has_upper_limit = true;

        /*std::cerr << std::fixed << std::setprecision(100)
            << "relative_sample_size: " << relative_sample_size << "\n"
            << "relative_sample_offset: " << relative_sample_offset << "\n"
            << "lower_limit_float: " << lower_limit_rational << "\n"
            << "upper_limit_float: " << upper_limit_rational << "\n"
            << "lower: " << lower << "\n"
            << "upper: " << upper << "\n";*/

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

            std::shared_ptr<ASTFunction> lower_function;
            std::shared_ptr<ASTFunction> upper_function;

            /// If sample and final are used together no need to calculate sampling expression twice.
            /// The first time it was calculated for final, because sample key is a part of the PK.
            /// So, assume that we already have calculated column.
            ASTPtr sampling_key_ast = metadata_snapshot->getSamplingKeyAST();

            if (select.final())
            {
                sampling_key_ast = std::make_shared<ASTIdentifier>(sampling_key.column_names[0]);
                /// We do spoil available_real_columns here, but it is not used later.
                available_real_columns.emplace_back(sampling_key.column_names[0], std::move(sampling_column_type));
            }

            if (has_lower_limit)
            {
                if (!key_condition.addCondition(sampling_key.column_names[0], Range::createLeftBounded(lower, true)))
                    throw Exception("Sampling column not in primary key", ErrorCodes::ILLEGAL_COLUMN);

                ASTPtr args = std::make_shared<ASTExpressionList>();
                args->children.push_back(sampling_key_ast);
                args->children.push_back(std::make_shared<ASTLiteral>(lower));

                lower_function = std::make_shared<ASTFunction>();
                lower_function->name = "greaterOrEquals";
                lower_function->arguments = args;
                lower_function->children.push_back(lower_function->arguments);

                sampling.filter_function = lower_function;
            }

            if (has_upper_limit)
            {
                if (!key_condition.addCondition(sampling_key.column_names[0], Range::createRightBounded(upper, false)))
                    throw Exception("Sampling column not in primary key", ErrorCodes::ILLEGAL_COLUMN);

                ASTPtr args = std::make_shared<ASTExpressionList>();
                args->children.push_back(sampling_key_ast);
                args->children.push_back(std::make_shared<ASTLiteral>(upper));

                upper_function = std::make_shared<ASTFunction>();
                upper_function->name = "less";
                upper_function->arguments = args;
                upper_function->children.push_back(upper_function->arguments);

                sampling.filter_function = upper_function;
            }

            if (has_lower_limit && has_upper_limit)
            {
                ASTPtr args = std::make_shared<ASTExpressionList>();
                args->children.push_back(lower_function);
                args->children.push_back(upper_function);

                sampling.filter_function = std::make_shared<ASTFunction>();
                sampling.filter_function->name = "and";
                sampling.filter_function->arguments = args;
                sampling.filter_function->children.push_back(sampling.filter_function->arguments);
            }

            ASTPtr query = sampling.filter_function;
            auto syntax_result = TreeRewriter(context).analyze(query, available_real_columns);
            sampling.filter_expression = ExpressionAnalyzer(sampling.filter_function, syntax_result, context).getActionsDAG(false);
        }
    }

    if (no_data)
    {
        LOG_DEBUG(log, "Sampling yields no data.");
        sampling.read_nothing = true;
    }

    return sampling;
}

std::optional<std::unordered_set<String>> MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(
    const MergeTreeData & data,
    const MergeTreeData::DataPartsVector & parts,
    const ASTPtr & query,
    ContextPtr context)
{
    std::unordered_set<String> part_values;
    ASTPtr expression_ast;
    auto virtual_columns_block = data.getBlockWithVirtualPartColumns(parts, true /* one_part */);

    // Generate valid expressions for filtering
    VirtualColumnUtils::prepareFilterBlockWithQuery(query, context, virtual_columns_block, expression_ast);

    // If there is still something left, fill the virtual block and do the filtering.
    if (expression_ast)
    {
        virtual_columns_block = data.getBlockWithVirtualPartColumns(parts, false /* one_part */);
        VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context, expression_ast);
        return VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");
    }

    return {};
}

void MergeTreeDataSelectExecutor::filterPartsByPartition(
    MergeTreeData::DataPartsVector & parts,
    const std::optional<std::unordered_set<String>> & part_values,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData & data,
    const SelectQueryInfo & query_info,
    const ContextPtr & context,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    Poco::Logger * log,
    ReadFromMergeTree::IndexStats & index_stats)
{
    const Settings & settings = context->getSettingsRef();
    std::optional<PartitionPruner> partition_pruner;
    std::optional<KeyCondition> minmax_idx_condition;
    DataTypes minmax_columns_types;
    if (metadata_snapshot->hasPartitionKey())
    {
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        auto minmax_columns_names = data.getMinMaxColumnsNames(partition_key);
        minmax_columns_types = data.getMinMaxColumnsTypes(partition_key);

        minmax_idx_condition.emplace(
            query_info, context, minmax_columns_names, data.getMinMaxExpr(partition_key, ExpressionActionsSettings::fromContext(context)));
        partition_pruner.emplace(metadata_snapshot, query_info, context, false /* strict */);

        if (settings.force_index_by_date && (minmax_idx_condition->alwaysUnknownOrTrue() && partition_pruner->isUseless()))
        {
            String msg = "Neither MinMax index by columns (";
            bool first = true;
            for (const String & col : minmax_columns_names)
            {
                if (first)
                    first = false;
                else
                    msg += ", ";
                msg += col;
            }
            msg += ") nor partition expr is used and setting 'force_index_by_date' is set";

            throw Exception(msg, ErrorCodes::INDEX_NOT_USED);
        }
    }

    auto query_context = context->hasQueryContext() ? context->getQueryContext() : context;
    PartFilterCounters part_filter_counters;
    if (query_context->getSettingsRef().allow_experimental_query_deduplication)
        selectPartsToReadWithUUIDFilter(
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
        selectPartsToRead(
            parts,
            part_values,
            minmax_idx_condition,
            minmax_columns_types,
            partition_pruner,
            max_block_numbers_to_read,
            part_filter_counters);

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
}

RangesInDataParts MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
    MergeTreeData::DataPartsVector && parts,
    StorageMetadataPtr metadata_snapshot,
    const SelectQueryInfo & query_info,
    const ContextPtr & context,
    const KeyCondition & key_condition,
    const MergeTreeReaderSettings & reader_settings,
    Poco::Logger * log,
    size_t num_streams,
    ReadFromMergeTree::IndexStats & index_stats,
    bool use_skip_indexes)
{
    RangesInDataParts parts_with_ranges(parts.size());
    const Settings & settings = context->getSettingsRef();

    /// Let's start analyzing all useful indices

    struct DataSkippingIndexAndCondition
    {
        MergeTreeIndexPtr index;
        MergeTreeIndexConditionPtr condition;
        std::atomic<size_t> total_granules{0};
        std::atomic<size_t> granules_dropped{0};
        std::atomic<size_t> total_parts{0};
        std::atomic<size_t> parts_dropped{0};

        DataSkippingIndexAndCondition(MergeTreeIndexPtr index_, MergeTreeIndexConditionPtr condition_)
            : index(index_), condition(condition_)
        {
        }
    };
    std::list<DataSkippingIndexAndCondition> useful_indices;

    if (use_skip_indexes)
    {
        for (const auto & index : metadata_snapshot->getSecondaryIndices())
        {
            auto index_helper = MergeTreeIndexFactory::instance().get(index);
            auto condition = index_helper->createIndexCondition(query_info, context);
            if (!condition->alwaysUnknownOrTrue())
                useful_indices.emplace_back(index_helper, condition);
        }
    }

    if (use_skip_indexes && settings.force_data_skipping_indices.changed)
    {
        const auto & indices = settings.force_data_skipping_indices.toString();

        Strings forced_indices;
        {
            Tokens tokens(&indices[0], &indices[indices.size()], settings.max_query_size);
            IParser::Pos pos(tokens, settings.max_parser_depth);
            Expected expected;
            if (!parseIdentifiersOrStringLiterals(pos, expected, forced_indices))
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse force_data_skipping_indices ('{}')", indices);
        }

        if (forced_indices.empty())
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "No indices parsed from force_data_skipping_indices ('{}')", indices);

        std::unordered_set<std::string> useful_indices_names;
        for (const auto & useful_index : useful_indices)
            useful_indices_names.insert(useful_index.index->index.name);

        for (const auto & index_name : forced_indices)
        {
            if (!useful_indices_names.count(index_name))
            {
                throw Exception(
                    ErrorCodes::INDEX_NOT_USED,
                    "Index {} is not used and setting 'force_data_skipping_indices' contains it",
                    backQuote(index_name));
            }
        }
    }

    std::atomic<size_t> sum_marks_pk = 0;
    std::atomic<size_t> sum_parts_pk = 0;

    /// Let's find what range to read from each part.
    {
        std::atomic<size_t> total_rows{0};

        SizeLimits limits;
        if (settings.read_overflow_mode == OverflowMode::THROW && settings.max_rows_to_read)
            limits = SizeLimits(settings.max_rows_to_read, 0, settings.read_overflow_mode);

        SizeLimits leaf_limits;
        if (settings.read_overflow_mode_leaf == OverflowMode::THROW && settings.max_rows_to_read_leaf)
            leaf_limits = SizeLimits(settings.max_rows_to_read_leaf, 0, settings.read_overflow_mode_leaf);

        auto process_part = [&](size_t part_index)
        {
            auto & part = parts[part_index];

            RangesInDataPart ranges(part, part_index);

            size_t total_marks_count = part->index_granularity.getMarksCountWithoutFinal();

            if (metadata_snapshot->hasPrimaryKey())
                ranges.ranges = markRangesFromPKRange(part, metadata_snapshot, key_condition, settings, log);
            else if (total_marks_count)
                ranges.ranges = MarkRanges{MarkRange{0, total_marks_count}};

            sum_marks_pk.fetch_add(ranges.getMarksCount(), std::memory_order_relaxed);

            if (!ranges.ranges.empty())
                sum_parts_pk.fetch_add(1, std::memory_order_relaxed);

            for (auto & index_and_condition : useful_indices)
            {
                if (ranges.ranges.empty())
                    break;

                index_and_condition.total_parts.fetch_add(1, std::memory_order_relaxed);

                size_t total_granules = 0;
                size_t granules_dropped = 0;
                ranges.ranges = filterMarksUsingIndex(
                    index_and_condition.index,
                    index_and_condition.condition,
                    part,
                    ranges.ranges,
                    settings,
                    reader_settings,
                    total_granules,
                    granules_dropped,
                    log);

                index_and_condition.total_granules.fetch_add(total_granules, std::memory_order_relaxed);
                index_and_condition.granules_dropped.fetch_add(granules_dropped, std::memory_order_relaxed);

                if (ranges.ranges.empty())
                    index_and_condition.parts_dropped.fetch_add(1, std::memory_order_relaxed);
            }

            if (!ranges.ranges.empty())
            {
                if (limits.max_rows || leaf_limits.max_rows)
                {
                    /// Fail fast if estimated number of rows to read exceeds the limit
                    auto current_rows_estimate = ranges.getRowsCount();
                    size_t prev_total_rows_estimate = total_rows.fetch_add(current_rows_estimate);
                    size_t total_rows_estimate = current_rows_estimate + prev_total_rows_estimate;
                    limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
                    leaf_limits.check(
                        total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
                }

                parts_with_ranges[part_index] = std::move(ranges);
            }
        };

        size_t num_threads = std::min(size_t(num_streams), parts.size());

        if (num_threads <= 1)
        {
            for (size_t part_index = 0; part_index < parts.size(); ++part_index)
                process_part(part_index);
        }
        else
        {
            /// Parallel loading of data parts.
            ThreadPool pool(num_threads);

            for (size_t part_index = 0; part_index < parts.size(); ++part_index)
                pool.scheduleOrThrowOnError([&, part_index, thread_group = CurrentThread::getGroup()]
                {
                    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachQueryIfNotDetached(););
                    if (thread_group)
                        CurrentThread::attachTo(thread_group);

                    process_part(part_index);
                });

            pool.wait();
        }

        /// Skip empty ranges.
        size_t next_part = 0;
        for (size_t part_index = 0; part_index < parts.size(); ++part_index)
        {
            auto & part = parts_with_ranges[part_index];
            if (!part.data_part)
                continue;

            if (next_part != part_index)
                std::swap(parts_with_ranges[next_part], part);

            ++next_part;
        }

        parts_with_ranges.resize(next_part);
    }

    if (metadata_snapshot->hasPrimaryKey())
    {
        auto description = key_condition.getDescription();

        index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::PrimaryKey,
            .condition = std::move(description.condition),
            .used_keys = std::move(description.used_keys),
            .num_parts_after = sum_parts_pk.load(std::memory_order_relaxed),
            .num_granules_after = sum_marks_pk.load(std::memory_order_relaxed)});
    }

    for (const auto & index_and_condition : useful_indices)
    {
        const auto & index_name = index_and_condition.index->index.name;
        LOG_DEBUG(
            log,
            "Index {} has dropped {}/{} granules.",
            backQuote(index_name),
            index_and_condition.granules_dropped,
            index_and_condition.total_granules);

        std::string description
            = index_and_condition.index->index.type + " GRANULARITY " + std::to_string(index_and_condition.index->index.granularity);

        index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::Skip,
            .name = index_name,
            .description = std::move(description),
            .num_parts_after = index_and_condition.total_parts - index_and_condition.parts_dropped,
            .num_granules_after = index_and_condition.total_granules - index_and_condition.granules_dropped});
    }

    return parts_with_ranges;
}

std::shared_ptr<QueryIdHolder> MergeTreeDataSelectExecutor::checkLimits(
    const MergeTreeData & data,
    const RangesInDataParts & parts_with_ranges,
    const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    // Check limitations. query_id is used as the quota RAII's resource key.
    String query_id;
    {
        const auto data_settings = data.getSettings();
        auto max_partitions_to_read
                = settings.max_partitions_to_read.changed ? settings.max_partitions_to_read : data_settings->max_partitions_to_read;
        if (max_partitions_to_read > 0)
        {
            std::set<String> partitions;
            for (const auto & part_with_ranges : parts_with_ranges)
                partitions.insert(part_with_ranges.data_part->info.partition_id);
            if (partitions.size() > size_t(max_partitions_to_read))
                throw Exception(
                        ErrorCodes::TOO_MANY_PARTITIONS,
                        "Too many partitions to read. Current {}, max {}",
                        partitions.size(),
                        max_partitions_to_read);
        }

        if (data_settings->max_concurrent_queries > 0 && data_settings->min_marks_to_honor_max_concurrent_queries > 0)
        {
            size_t sum_marks = 0;
            for (const auto & part : parts_with_ranges)
                sum_marks += part.getMarksCount();

            if (sum_marks >= data_settings->min_marks_to_honor_max_concurrent_queries)
            {
                query_id = context->getCurrentQueryId();
                if (!query_id.empty())
                    data.insertQueryIdOrThrow(query_id, data_settings->max_concurrent_queries);
            }
        }
    }

    if (!query_id.empty())
        return std::make_shared<QueryIdHolder>(query_id, data);

    return nullptr;
}

static void selectColumnNames(
    const Names & column_names_to_return,
    const MergeTreeData & data,
    Names & real_column_names,
    Names & virt_column_names,
    bool & sample_factor_column_queried)
{
    sample_factor_column_queried = false;

    for (const String & name : column_names_to_return)
    {
        if (name == "_part")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_part_index")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_partition_id")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_part_uuid")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_partition_value")
        {
            if (!typeid_cast<const DataTypeTuple *>(data.getPartitionValueType().get()))
            {
                throw Exception(
                    ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                    "Missing column `_partition_value` because there is no partition column in table {}",
                    data.getStorageID().getTableName());
            }

            virt_column_names.push_back(name);
        }
        else if (name == "_sample_factor")
        {
            sample_factor_column_queried = true;
            virt_column_names.push_back(name);
        }
        else
        {
            real_column_names.push_back(name);
        }
    }
}

size_t MergeTreeDataSelectExecutor::estimateNumMarksToRead(
    MergeTreeData::DataPartsVector parts,
    const Names & column_names_to_return,
    const StorageMetadataPtr & metadata_snapshot_base,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    unsigned num_streams,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read) const
{
    size_t total_parts = parts.size();
    if (total_parts == 0)
        return 0;

    Names real_column_names;
    Names virt_column_names;
    /// If query contains restrictions on the virtual column `_part` or `_part_index`, select only parts suitable for it.
    /// The virtual column `_sample_factor` (which is equal to 1 / used sample rate) can be requested in the query.
    bool sample_factor_column_queried = false;

    selectColumnNames(column_names_to_return, data, real_column_names, virt_column_names, sample_factor_column_queried);

    auto part_values = filterPartsByVirtualColumns(data, parts, query_info.query, context);
    if (part_values && part_values->empty())
        return 0;

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (real_column_names.empty())
    {
        NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
        real_column_names.push_back(ExpressionActions::getSmallestColumn(available_real_columns));
    }

    metadata_snapshot->check(real_column_names, data.getVirtuals(), data.getStorageID());

    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    Names primary_key_columns = primary_key.column_names;
    KeyCondition key_condition(query_info, context, primary_key_columns, primary_key.expression);

    if (key_condition.alwaysUnknownOrTrue())
    {
        size_t total_marks = 0;
        for (const auto & part : parts)
            total_marks += part->index_granularity.getMarksCountWithoutFinal();

        return total_marks;
    }

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    ReadFromMergeTree::IndexStats index_stats;

    filterPartsByPartition(
        parts, part_values, metadata_snapshot_base, data, query_info,
        context, max_block_numbers_to_read.get(), log, index_stats);

    auto sampling = MergeTreeDataSelectExecutor::getSampling(
        select, metadata_snapshot->getColumns().getAllPhysical(), parts, key_condition,
        data, metadata_snapshot, context, sample_factor_column_queried, log);

    if (sampling.read_nothing)
        return 0;

    /// Do not init. It is not used (cause skip index is ignored)
    MergeTreeReaderSettings reader_settings;

    auto parts_with_ranges = filterPartsByPrimaryKeyAndSkipIndexes(
        std::move(parts),
        metadata_snapshot,
        query_info,
        context,
        key_condition,
        reader_settings,
        log,
        num_streams,
        index_stats,
        false);

    return index_stats.back().num_granules_after;
}

QueryPlanPtr MergeTreeDataSelectExecutor::readFromParts(
    MergeTreeData::DataPartsVector parts,
    const Names & column_names_to_return,
    const StorageMetadataPtr & metadata_snapshot_base,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const UInt64 max_block_size,
    const unsigned num_streams,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read) const
{
    size_t total_parts = parts.size();
    if (total_parts == 0)
        return std::make_unique<QueryPlan>();

    Names real_column_names;
    Names virt_column_names;
    /// If query contains restrictions on the virtual column `_part` or `_part_index`, select only parts suitable for it.
    /// The virtual column `_sample_factor` (which is equal to 1 / used sample rate) can be requested in the query.
    bool sample_factor_column_queried = false;

    selectColumnNames(column_names_to_return, data, real_column_names, virt_column_names, sample_factor_column_queried);

    auto read_from_merge_tree = std::make_unique<ReadFromMergeTree>(
        parts,
        real_column_names,
        virt_column_names,
        data,
        query_info,
        metadata_snapshot,
        metadata_snapshot_base,
        context,
        max_block_size,
        num_streams,
        sample_factor_column_queried,
        max_block_numbers_to_read,
        log
    );

    QueryPlanPtr plan = std::make_unique<QueryPlan>();
    plan->addStep(std::move(read_from_merge_tree));
    return plan;
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
    else
        return std::max(res, (bytes_setting + bytes_granularity - 1) / bytes_granularity);
}

/// Same as roundRowsOrBytesToMarks() but do not return more then max_marks
size_t MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
    size_t rows_setting,
    size_t bytes_setting,
    size_t rows_granularity,
    size_t bytes_granularity,
    size_t max_marks)
{
    size_t marks = 1;

    if (rows_setting + rows_granularity <= rows_setting) /// overflow
        marks = max_marks;
    else if (rows_setting)
        marks = (rows_setting + rows_granularity - 1) / rows_granularity;

    if (bytes_granularity == 0)
        return marks;
    else
    {
        /// Overflow
        if (bytes_setting + bytes_granularity <= bytes_setting) /// overflow
            return max_marks;
        if (bytes_setting)
            return std::max(marks, (bytes_setting + bytes_granularity - 1) / bytes_granularity);
        else
            return marks;
    }
}


/// Calculates a set of mark ranges, that could possibly contain keys, required by condition.
/// In other words, it removes subranges from whole range, that definitely could not contain required keys.
MarkRanges MergeTreeDataSelectExecutor::markRangesFromPKRange(
    const MergeTreeData::DataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const KeyCondition & key_condition,
    const Settings & settings,
    Poco::Logger * log)
{
    MarkRanges res;

    size_t marks_count = part->index_granularity.getMarksCount();
    const auto & index = part->index;
    if (marks_count == 0)
        return res;

    bool has_final_mark = part->index_granularity.hasFinalMark();

    /// If index is not used.
    if (key_condition.alwaysUnknownOrTrue())
    {
        if (has_final_mark)
            res.push_back(MarkRange(0, marks_count - 1));
        else
            res.push_back(MarkRange(0, marks_count));

        return res;
    }

    size_t used_key_size = key_condition.getMaxKeyColumn() + 1;

    std::function<void(size_t, size_t, FieldRef &)> create_field_ref;
    /// If there are no monotonic functions, there is no need to save block reference.
    /// Passing explicit field to FieldRef allows to optimize ranges and shows better performance.
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    if (key_condition.hasMonotonicFunctionsChain())
    {
        auto index_columns = std::make_shared<ColumnsWithTypeAndName>();
        for (size_t i = 0; i < used_key_size; ++i)
            index_columns->emplace_back(ColumnWithTypeAndName{index[i], primary_key.data_types[i], primary_key.column_names[i]});

        create_field_ref = [index_columns](size_t row, size_t column, FieldRef & field)
        {
            field = {index_columns.get(), row, column};
        };
    }
    else
    {
        create_field_ref = [&index](size_t row, size_t column, FieldRef & field)
        {
            index[column]->get(row, field);
        };
    }

    /// NOTE Creating temporary Field objects to pass to KeyCondition.
    std::vector<FieldRef> index_left(used_key_size);
    std::vector<FieldRef> index_right(used_key_size);

    auto may_be_true_in_range = [&](MarkRange & range)
    {
        if (range.end == marks_count && !has_final_mark)
        {
            for (size_t i = 0; i < used_key_size; ++i)
                create_field_ref(range.begin, i, index_left[i]);

            return key_condition.mayBeTrueAfter(
                used_key_size, index_left.data(), primary_key.data_types);
        }

        if (has_final_mark && range.end == marks_count)
            range.end -= 1; /// Remove final empty mark. It's useful only for primary key condition.

        for (size_t i = 0; i < used_key_size; ++i)
        {
            create_field_ref(range.begin, i, index_left[i]);
            create_field_ref(range.end, i, index_right[i]);
        }

        return key_condition.mayBeTrueInRange(
            used_key_size, index_left.data(), index_right.data(), primary_key.data_types);
    };

    if (!key_condition.matchesExactContinuousRange())
    {
        // Do exclusion search, where we drop ranges that do not match

        size_t min_marks_for_seek = roundRowsOrBytesToMarks(
            settings.merge_tree_min_rows_for_seek,
            settings.merge_tree_min_bytes_for_seek,
            part->index_granularity_info.fixed_index_granularity,
            part->index_granularity_info.index_granularity_bytes);

        /** There will always be disjoint suspicious segments on the stack, the leftmost one at the top (back).
        * At each step, take the left segment and check if it fits.
        * If fits, split it into smaller ones and put them on the stack. If not, discard it.
        * If the segment is already of one mark length, add it to response and discard it.
        */
        std::vector<MarkRange> ranges_stack = { {0, marks_count} };

        size_t steps = 0;

        while (!ranges_stack.empty())
        {
            MarkRange range = ranges_stack.back();
            ranges_stack.pop_back();

            steps++;

            if (!may_be_true_in_range(range))
                continue;

            if (range.end == range.begin + 1)
            {
                /// We saw a useful gap between neighboring marks. Either add it to the last range, or start a new range.
                if (res.empty() || range.begin - res.back().end > min_marks_for_seek)
                    res.push_back(range);
                else
                    res.back().end = range.end;
            }
            else
            {
                /// Break the segment and put the result on the stack from right to left.
                size_t step = (range.end - range.begin - 1) / settings.merge_tree_coarse_index_granularity + 1;
                size_t end;

                for (end = range.end; end > range.begin + step; end -= step)
                    ranges_stack.emplace_back(end - step, end);

                ranges_stack.emplace_back(range.begin, end);
            }
        }

        LOG_TRACE(log, "Used generic exclusion search over index for part {} with {} steps", part->name, steps);
    }
    else
    {
        /// In case when SELECT's predicate defines a single continuous interval of keys,
        /// we can use binary search algorithm to find the left and right endpoint key marks of such interval.
        /// The returned value is the minimum range of marks, containing all keys for which KeyCondition holds

        LOG_TRACE(log, "Running binary search on index range for part {} ({} marks)", part->name, marks_count);

        size_t steps = 0;

        MarkRange result_range;

        size_t searched_left = 0;
        size_t searched_right = marks_count;

        while (searched_left + 1 < searched_right)
        {
            const size_t middle = (searched_left + searched_right) / 2;
            MarkRange range(0, middle);
            if (may_be_true_in_range(range))
                searched_right = middle;
            else
                searched_left = middle;
            ++steps;
        }
        result_range.begin = searched_left;
        LOG_TRACE(log, "Found (LEFT) boundary mark: {}", searched_left);

        searched_right = marks_count;
        while (searched_left + 1 < searched_right)
        {
            const size_t middle = (searched_left + searched_right) / 2;
            MarkRange range(middle, marks_count);
            if (may_be_true_in_range(range))
                searched_left = middle;
            else
                searched_right = middle;
            ++steps;
        }
        result_range.end = searched_right;
        LOG_TRACE(log, "Found (RIGHT) boundary mark: {}", searched_right);

        if (result_range.begin < result_range.end && may_be_true_in_range(result_range))
            res.emplace_back(std::move(result_range));

        LOG_TRACE(log, "Found {} range in {} steps", res.empty() ? "empty" : "continuous", steps);
    }

    return res;
}


MarkRanges MergeTreeDataSelectExecutor::filterMarksUsingIndex(
    MergeTreeIndexPtr index_helper,
    MergeTreeIndexConditionPtr condition,
    MergeTreeData::DataPartPtr part,
    const MarkRanges & ranges,
    const Settings & settings,
    const MergeTreeReaderSettings & reader_settings,
    size_t & total_granules,
    size_t & granules_dropped,
    Poco::Logger * log)
{
    if (!part->volume->getDisk()->exists(part->getFullRelativePath() + index_helper->getFileName() + ".idx"))
    {
        LOG_DEBUG(log, "File for index {} does not exist. Skipping it.", backQuote(index_helper->index.name));
        return ranges;
    }

    auto index_granularity = index_helper->index.granularity;

    const size_t min_marks_for_seek = roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_seek,
        settings.merge_tree_min_bytes_for_seek,
        part->index_granularity_info.fixed_index_granularity,
        part->index_granularity_info.index_granularity_bytes);

    size_t marks_count = part->getMarksCount();
    size_t final_mark = part->index_granularity.hasFinalMark();
    size_t index_marks_count = (marks_count - final_mark + index_granularity - 1) / index_granularity;

    MergeTreeIndexReader reader(
        index_helper, part,
        index_marks_count,
        ranges,
        reader_settings);

    MarkRanges res;

    /// Some granules can cover two or more ranges,
    /// this variable is stored to avoid reading the same granule twice.
    MergeTreeIndexGranulePtr granule = nullptr;
    size_t last_index_mark = 0;
    for (const auto & range : ranges)
    {
        MarkRange index_range(
                range.begin / index_granularity,
                (range.end + index_granularity - 1) / index_granularity);

        if (last_index_mark != index_range.begin || !granule)
            reader.seek(index_range.begin);

        total_granules += index_range.end - index_range.begin;

        for (size_t index_mark = index_range.begin; index_mark < index_range.end; ++index_mark)
        {
            if (index_mark != index_range.begin || !granule || last_index_mark != index_range.begin)
                granule = reader.read();

            MarkRange data_range(
                    std::max(range.begin, index_mark * index_granularity),
                    std::min(range.end, (index_mark + 1) * index_granularity));

            if (!condition->mayBeTrueOnGranule(granule))
            {
                ++granules_dropped;
                continue;
            }

            if (res.empty() || res.back().end - data_range.begin > min_marks_for_seek)
                res.push_back(data_range);
            else
                res.back().end = data_range.end;
        }

        last_index_mark = index_range.end - 1;
    }

    return res;
}

void MergeTreeDataSelectExecutor::selectPartsToRead(
    MergeTreeData::DataPartsVector & parts,
    const std::optional<std::unordered_set<String>> & part_values,
    const std::optional<KeyCondition> & minmax_idx_condition,
    const DataTypes & minmax_columns_types,
    std::optional<PartitionPruner> & partition_pruner,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    PartFilterCounters & counters)
{
    MergeTreeData::DataPartsVector prev_parts;
    std::swap(prev_parts, parts);
    for (const auto & part_or_projection : prev_parts)
    {
        const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
        if (part_values && part_values->find(part->name) == part_values->end())
            continue;

        if (part->isEmpty())
            continue;

        if (max_block_numbers_to_read)
        {
            auto blocks_iterator = max_block_numbers_to_read->find(part->info.partition_id);
            if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                continue;
        }

        size_t num_granules = part->getMarksCount();
        if (num_granules && part->index_granularity.hasFinalMark())
            --num_granules;

        counters.num_initial_selected_parts += 1;
        counters.num_initial_selected_granules += num_granules;

        if (minmax_idx_condition && !minmax_idx_condition->checkInHyperrectangle(
                part->minmax_idx.hyperrectangle, minmax_columns_types).can_be_true)
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

        parts.push_back(part_or_projection);
    }
}

void MergeTreeDataSelectExecutor::selectPartsToReadWithUUIDFilter(
    MergeTreeData::DataPartsVector & parts,
    const std::optional<std::unordered_set<String>> & part_values,
    MergeTreeData::PinnedPartUUIDsPtr pinned_part_uuids,
    const std::optional<KeyCondition> & minmax_idx_condition,
    const DataTypes & minmax_columns_types,
    std::optional<PartitionPruner> & partition_pruner,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    ContextPtr query_context,
    PartFilterCounters & counters,
    Poco::Logger * log)
{
    const Settings & settings = query_context->getSettings();

    /// process_parts prepare parts that have to be read for the query,
    /// returns false if duplicated parts' UUID have been met
    auto select_parts = [&] (MergeTreeData::DataPartsVector & selected_parts) -> bool
    {
        auto ignored_part_uuids = query_context->getIgnoredPartUUIDs();
        std::unordered_set<UUID> temp_part_uuids;

        MergeTreeData::DataPartsVector prev_parts;
        std::swap(prev_parts, selected_parts);
        for (const auto & part_or_projection : prev_parts)
        {
            const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
            if (part_values && part_values->find(part->name) == part_values->end())
                continue;

            if (part->isEmpty())
                continue;

            if (max_block_numbers_to_read)
            {
                auto blocks_iterator = max_block_numbers_to_read->find(part->info.partition_id);
                if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                    continue;
            }

            /// Skip the part if its uuid is meant to be excluded
            if (part->uuid != UUIDHelpers::Nil && ignored_part_uuids->has(part->uuid))
                continue;

            size_t num_granules = part->getMarksCount();
            if (num_granules && part->index_granularity.hasFinalMark())
                --num_granules;

            counters.num_initial_selected_parts += 1;
            counters.num_initial_selected_granules += num_granules;

            if (minmax_idx_condition
                && !minmax_idx_condition->checkInHyperrectangle(part->minmax_idx.hyperrectangle, minmax_columns_types)
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
            if (part->uuid != UUIDHelpers::Nil)
            {
                if (settings.experimental_query_deduplication_send_all_part_uuids || pinned_part_uuids->contains(part->uuid))
                {
                    auto result = temp_part_uuids.insert(part->uuid);
                    if (!result.second)
                        throw Exception("Found a part with the same UUID on the same replica.", ErrorCodes::LOGICAL_ERROR);
                }
            }

            selected_parts.push_back(part_or_projection);
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
    auto needs_retry = !select_parts(parts);

    /// If any duplicated part UUIDs met during the first step, try to ignore them in second pass.
    /// This may happen when `prefer_localhost_replica` is set and "distributed" stage runs in the same process with "remote" stage.
    if (needs_retry)
    {
        LOG_DEBUG(log, "Found duplicate uuids locally, will retry part selection without them");

        counters = PartFilterCounters();

        /// Second attempt didn't help, throw an exception
        if (!select_parts(parts))
            throw Exception("Found duplicate UUIDs while processing query.", ErrorCodes::DUPLICATED_PART_UUIDS);
    }
}

}
