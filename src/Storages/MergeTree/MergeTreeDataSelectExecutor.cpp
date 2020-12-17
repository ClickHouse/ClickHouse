#include <boost/rational.hpp>   /// For calculations related to sampling coefficients.
#include <ext/scope_guard.h>
#include <optional>
#include <unordered_set>

#include <Poco/File.h>

#include <Common/FieldVisitors.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/KeyCondition.h>
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
#include <Processors/QueryPlan/AddingConstColumnStep.h>
#include <Processors/QueryPlan/ReverseRowsStep.h>
#include <Processors/QueryPlan/MergingSortedStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/MergingFinal.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/VirtualColumnUtils.h>

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
}


MergeTreeDataSelectExecutor::MergeTreeDataSelectExecutor(const MergeTreeData & data_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + " (SelectExecutor)"))
{
}


/// Construct a block consisting only of possible values of virtual columns
static Block getBlockWithPartColumn(const MergeTreeData::DataPartsVector & parts)
{
    auto column = ColumnString::create();

    for (const auto & part : parts)
        column->insert(part->name);

    return Block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "_part")};
}


size_t MergeTreeDataSelectExecutor::getApproximateTotalRowsToRead(
    const MergeTreeData::DataPartsVector & parts,
    const StorageMetadataPtr & metadata_snapshot,
    const KeyCondition & key_condition,
    const Settings & settings) const
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
    const Context & context,
    const UInt64 max_block_size,
    const unsigned num_streams,
    const PartitionIdToMaxBlock * max_block_numbers_to_read) const
{
    return readFromParts(
        data.getDataPartsVector(), column_names_to_return, metadata_snapshot,
        query_info, context, max_block_size, num_streams,
        max_block_numbers_to_read);
}

QueryPlanPtr MergeTreeDataSelectExecutor::readFromParts(
    MergeTreeData::DataPartsVector parts,
    const Names & column_names_to_return,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    const UInt64 max_block_size,
    const unsigned num_streams,
    const PartitionIdToMaxBlock * max_block_numbers_to_read) const
{
    /// If query contains restrictions on the virtual column `_part` or `_part_index`, select only parts suitable for it.
    /// The virtual column `_sample_factor` (which is equal to 1 / used sample rate) can be requested in the query.
    Names virt_column_names;
    Names real_column_names;

    bool part_column_queried = false;

    bool sample_factor_column_queried = false;
    Float64 used_sample_factor = 1;

    for (const String & name : column_names_to_return)
    {
        if (name == "_part")
        {
            part_column_queried = true;
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

    NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (real_column_names.empty())
        real_column_names.push_back(ExpressionActions::getSmallestColumn(available_real_columns));

    /// If `_part` virtual column is requested, we try to use it as an index.
    Block virtual_columns_block = getBlockWithPartColumn(parts);
    if (part_column_queried)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, context);

    auto part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");

    metadata_snapshot->check(real_column_names, data.getVirtuals(), data.getStorageID());

    const Settings & settings = context.getSettingsRef();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    Names primary_key_columns = primary_key.column_names;

    KeyCondition key_condition(query_info, context, primary_key_columns, primary_key.expression);

    if (settings.force_primary_key && key_condition.alwaysUnknownOrTrue())
    {
        throw Exception(ErrorCodes::INDEX_NOT_USED, "Primary key ({}) is not used and setting 'force_primary_key' is set.",
                        boost::algorithm::join(primary_key_columns, ", "));
    }

    std::optional<KeyCondition> minmax_idx_condition;
    std::optional<PartitionPruner> partition_pruner;
    if (data.minmax_idx_expr)
    {
        minmax_idx_condition.emplace(query_info, context, data.minmax_idx_columns, data.minmax_idx_expr);
        partition_pruner.emplace(metadata_snapshot->getPartitionKey(), query_info, context, false /* strict */);

        if (settings.force_index_by_date && (minmax_idx_condition->alwaysUnknownOrTrue() && partition_pruner->isUseless()))
        {
            String msg = "Neither MinMax index by columns (";
            bool first = true;
            for (const String & col : data.minmax_idx_columns)
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

    /// Select the parts in which there can be data that satisfy `minmax_idx_condition` and that match the condition on `_part`,
    ///  as well as `max_block_number_to_read`.
    {
        auto prev_parts = parts;
        parts.clear();

        for (const auto & part : prev_parts)
        {
            if (part_values.find(part->name) == part_values.end())
                continue;

            if (part->isEmpty())
                continue;

            if (minmax_idx_condition && !minmax_idx_condition->checkInHyperrectangle(
                    part->minmax_idx.hyperrectangle, data.minmax_idx_column_types).can_be_true)
                continue;

            if (partition_pruner)
            {
                if (partition_pruner->canBePruned(part))
                    continue;
            }

            if (max_block_numbers_to_read)
            {
                auto blocks_iterator = max_block_numbers_to_read->find(part->info.partition_id);
                if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                    continue;
            }

            parts.push_back(part);
        }
    }

    /// Sampling.
    Names column_names_to_read = real_column_names;
    std::shared_ptr<ASTFunction> filter_function;
    ActionsDAGPtr filter_expression;

    RelativeSize relative_sample_size = 0;
    RelativeSize relative_sample_offset = 0;

    const auto & select = query_info.query->as<ASTSelectQuery &>();

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

        /// Convert absolute value of the sampling (in form `SAMPLE 1000000` - how many rows to read) into the relative `SAMPLE 0.1` (how much data to read).
        size_t approx_total_rows = 0;
        if (relative_sample_size > 1 || relative_sample_offset > 1)
            approx_total_rows = getApproximateTotalRowsToRead(parts, metadata_snapshot, key_condition, settings);

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
        return {};
    }

    bool use_sampling = relative_sample_size > 0 || (settings.parallel_replicas_count > 1 && data.supportsSampling());
    bool no_data = false;   /// There is nothing left after sampling.

    if (use_sampling)
    {
        if (sample_factor_column_queried && relative_sample_size != RelativeSize(0))
            used_sample_factor = 1.0 / boost::rational_cast<Float64>(relative_sample_size);

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
            use_sampling = false;
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

                filter_function = lower_function;
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

                filter_function = upper_function;
            }

            if (has_lower_limit && has_upper_limit)
            {
                ASTPtr args = std::make_shared<ASTExpressionList>();
                args->children.push_back(lower_function);
                args->children.push_back(upper_function);

                filter_function = std::make_shared<ASTFunction>();
                filter_function->name = "and";
                filter_function->arguments = args;
                filter_function->children.push_back(filter_function->arguments);
            }

            ASTPtr query = filter_function;
            auto syntax_result = TreeRewriter(context).analyze(query, available_real_columns);
            filter_expression = ExpressionAnalyzer(filter_function, syntax_result, context).getActionsDAG(false);

            if (!select.final())
            {
                /// Add columns needed for `sample_by_ast` to `column_names_to_read`.
                /// Skip this if final was used, because such columns were already added from PK.
                std::vector<String> add_columns = filter_expression->getRequiredColumns().getNames();
                column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
                std::sort(column_names_to_read.begin(), column_names_to_read.end());
                column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()),
                                           column_names_to_read.end());
            }
        }
    }

    if (no_data)
    {
        LOG_DEBUG(log, "Sampling yields no data.");
        return std::make_unique<QueryPlan>();
    }

    LOG_DEBUG(log, "Key condition: {}", key_condition.toString());
    if (minmax_idx_condition)
        LOG_DEBUG(log, "MinMax index condition: {}", minmax_idx_condition->toString());

    MergeTreeReaderSettings reader_settings =
    {
        .min_bytes_to_use_direct_io = settings.min_bytes_to_use_direct_io,
        .min_bytes_to_use_mmap_io = settings.min_bytes_to_use_mmap_io,
        .max_read_buffer_size = settings.max_read_buffer_size,
        .save_marks_in_cache = true
    };

    /// PREWHERE
    String prewhere_column;
    if (select.prewhere())
        prewhere_column = select.prewhere()->getColumnName();

    std::vector<std::pair<MergeTreeIndexPtr, MergeTreeIndexConditionPtr>> useful_indices;

    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        auto index_helper = MergeTreeIndexFactory::instance().get(index);
        auto condition = index_helper->createIndexCondition(query_info, context);
        if (!condition->alwaysUnknownOrTrue())
            useful_indices.emplace_back(index_helper, condition);
    }

    if (settings.force_data_skipping_indices.changed)
    {
        const auto & indices = settings.force_data_skipping_indices.toString();

        Strings forced_indices;
        {
            Tokens tokens(&indices[0], &indices[indices.size()], settings.max_query_size);
            IParser::Pos pos(tokens, settings.max_parser_depth);
            Expected expected;
            if (!parseIdentifiersOrStringLiterals(pos, expected, forced_indices))
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                    "Cannot parse force_data_skipping_indices ('{}')", indices);
        }

        if (forced_indices.empty())
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "No indices parsed from force_data_skipping_indices ('{}')", indices);

        std::unordered_set<std::string> useful_indices_names;
        for (const auto & useful_index : useful_indices)
            useful_indices_names.insert(useful_index.first->index.name);

        for (const auto & index_name : forced_indices)
        {
            if (!useful_indices_names.count(index_name))
            {
                throw Exception(ErrorCodes::INDEX_NOT_USED,
                    "Index {} is not used and setting 'force_data_skipping_indices' contains it",
                    backQuote(index_name));
            }
        }
    }

    RangesInDataParts parts_with_ranges(parts.size());
    size_t sum_marks = 0;
    std::atomic<size_t> sum_marks_pk = 0;
    size_t sum_ranges = 0;

    /// Let's find what range to read from each part.
    {
        std::atomic<size_t> total_rows {0};

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

            if (metadata_snapshot->hasPrimaryKey())
                ranges.ranges = markRangesFromPKRange(part, metadata_snapshot, key_condition, settings, log);
            else
            {
                size_t total_marks_count = part->getMarksCount();
                if (total_marks_count)
                {
                    if (part->index_granularity.hasFinalMark())
                        --total_marks_count;
                    ranges.ranges = MarkRanges{MarkRange{0, total_marks_count}};
                }
            }

            sum_marks_pk.fetch_add(ranges.getMarksCount(), std::memory_order_relaxed);

            for (const auto & index_and_condition : useful_indices)
                ranges.ranges = filterMarksUsingIndex(
                        index_and_condition.first, index_and_condition.second, part, ranges.ranges, settings, reader_settings, log);

            if (!ranges.ranges.empty())
            {
                if (limits.max_rows || leaf_limits.max_rows)
                {
                    /// Fail fast if estimated number of rows to read exceeds the limit
                    auto current_rows_estimate = ranges.getRowsCount();
                    size_t prev_total_rows_estimate = total_rows.fetch_add(current_rows_estimate);
                    size_t total_rows_estimate = current_rows_estimate + prev_total_rows_estimate;
                    limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
                    leaf_limits.check(total_rows_estimate, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
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
                pool.scheduleOrThrowOnError([&, part_index, thread_group = CurrentThread::getGroup()] {
                    SCOPE_EXIT(
                        if (thread_group)
                            CurrentThread::detachQueryIfNotDetached();
                    );
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

            sum_ranges += part.ranges.size();
            sum_marks += part.getMarksCount();

            if (next_part != part_index)
                std::swap(parts_with_ranges[next_part], part);

            ++next_part;
        }

        parts_with_ranges.resize(next_part);
    }

    LOG_DEBUG(log, "Selected {} parts by partition key, {} parts by primary key, {} marks by primary key, {} marks to read from {} ranges", parts.size(), parts_with_ranges.size(), sum_marks_pk.load(std::memory_order_relaxed), sum_marks, sum_ranges);

    if (parts_with_ranges.empty())
        return std::make_unique<QueryPlan>();

    ProfileEvents::increment(ProfileEvents::SelectedParts, parts_with_ranges.size());
    ProfileEvents::increment(ProfileEvents::SelectedRanges, sum_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, sum_marks);

    QueryPlanPtr plan;

    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    ActionsDAGPtr result_projection;

    if (select.final())
    {
        /// Add columns needed to calculate the sorting expression and the sign.
        std::vector<String> add_columns = metadata_snapshot->getColumnsRequiredForSortingKey();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

        if (!data.merging_params.sign_column.empty())
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty())
            column_names_to_read.push_back(data.merging_params.version_column);

        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());

        plan = spreadMarkRangesAmongStreamsFinal(
            std::move(parts_with_ranges),
            num_streams,
            column_names_to_read,
            metadata_snapshot,
            max_block_size,
            settings.use_uncompressed_cache,
            query_info,
            virt_column_names,
            settings,
            reader_settings,
            result_projection);
    }
    else if ((settings.optimize_read_in_order || settings.optimize_aggregation_in_order) && query_info.input_order_info)
    {
        size_t prefix_size = query_info.input_order_info->order_key_prefix_descr.size();
        auto order_key_prefix_ast = metadata_snapshot->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, metadata_snapshot->getColumns().getAllPhysical());
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false);

        plan = spreadMarkRangesAmongStreamsWithOrder(
            std::move(parts_with_ranges),
            num_streams,
            column_names_to_read,
            metadata_snapshot,
            max_block_size,
            settings.use_uncompressed_cache,
            query_info,
            sorting_key_prefix_expr,
            virt_column_names,
            settings,
            reader_settings,
            result_projection);
    }
    else
    {
        plan = spreadMarkRangesAmongStreams(
            std::move(parts_with_ranges),
            num_streams,
            column_names_to_read,
            metadata_snapshot,
            max_block_size,
            settings.use_uncompressed_cache,
            query_info,
            virt_column_names,
            settings,
            reader_settings);
    }

    if (!plan)
        return std::make_unique<QueryPlan>();

    if (use_sampling)
    {
        auto sampling_step = std::make_unique<FilterStep>(
                plan->getCurrentDataStream(),
                filter_expression,
                filter_function->getColumnName(),
                false);

        sampling_step->setStepDescription("Sampling");
        plan->addStep(std::move(sampling_step));
    }

    if (result_projection)
    {
        auto projection_step = std::make_unique<ExpressionStep>(plan->getCurrentDataStream(), result_projection);
        projection_step->setStepDescription("Remove unused columns after reading from storage");
        plan->addStep(std::move(projection_step));
    }

    /// By the way, if a distributed query or query to a Merge table is made, then the `_sample_factor` column can have different values.
    if (sample_factor_column_queried)
    {
        ColumnWithTypeAndName column;
        column.name = "_sample_factor";
        column.type = std::make_shared<DataTypeFloat64>();
        column.column = column.type->createColumnConst(0, Field(used_sample_factor));

        auto adding_column = std::make_unique<AddingConstColumnStep>(plan->getCurrentDataStream(), std::move(column));
        adding_column->setStepDescription("Add _sample_factor column");
        plan->addStep(std::move(adding_column));
    }

    if (query_info.prewhere_info && query_info.prewhere_info->remove_columns_actions)
    {
        auto expression_step = std::make_unique<ExpressionStep>(
                plan->getCurrentDataStream(),
                query_info.prewhere_info->remove_columns_actions->getActionsDAG().clone());

        expression_step->setStepDescription("Remove unused columns after PREWHERE");
        plan->addStep(std::move(expression_step));
    }

    return plan;
}

namespace
{

size_t roundRowsOrBytesToMarks(
    size_t rows_setting,
    size_t bytes_setting,
    size_t rows_granularity,
    size_t bytes_granularity)
{
    /// Marks are placed whenever threshold on rows or bytes is met.
    /// So we have to return the number of marks on whatever estimate is higher - by rows or by bytes.

    size_t res = (rows_setting + rows_granularity - 1) / rows_granularity;

    if (bytes_granularity == 0)
        return res;
    else
        return std::max(res, (bytes_setting + bytes_granularity - 1) / bytes_granularity);
}

}

static QueryPlanPtr createPlanFromPipe(Pipe pipe, const std::string & description = "")
{
    auto plan = std::make_unique<QueryPlan>();

    std::string storage_name = "MergeTree";
    if (!description.empty())
        storage_name += ' ' + description;

    auto step = std::make_unique<ReadFromStorageStep>(std::move(pipe), storage_name);
    plan->addStep(std::move(step));
    return plan;
}

QueryPlanPtr MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreams(
    RangesInDataParts && parts,
    size_t num_streams,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 max_block_size,
    bool use_uncompressed_cache,
    const SelectQueryInfo & query_info,
    const Names & virt_columns,
    const Settings & settings,
    const MergeTreeReaderSettings & reader_settings) const
{
    /// Count marks for each part.
    std::vector<size_t> sum_marks_in_parts(parts.size());
    size_t sum_marks = 0;
    size_t total_rows = 0;

    const auto data_settings = data.getSettings();
    size_t adaptive_parts = 0;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        total_rows += parts[i].getRowsCount();
        sum_marks_in_parts[i] = parts[i].getMarksCount();
        sum_marks += sum_marks_in_parts[i];

        if (parts[i].data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts > parts.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        settings.merge_tree_max_rows_to_use_cache,
        settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    const size_t min_marks_for_concurrent_read = roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_concurrent_read,
        settings.merge_tree_min_bytes_for_concurrent_read,
        data_settings->index_granularity,
        index_granularity_bytes);

    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    if (0 == sum_marks)
        return {};

    if (num_streams > 1)
    {
        /// Parallel query execution.
        Pipes res;

        /// Reduce the number of num_streams if the data is small.
        if (sum_marks < num_streams * min_marks_for_concurrent_read && parts.size() < num_streams)
            num_streams = std::max((sum_marks + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read, parts.size());

        MergeTreeReadPoolPtr pool = std::make_shared<MergeTreeReadPool>(
            num_streams,
            sum_marks,
            min_marks_for_concurrent_read,
            std::move(parts),
            data,
            metadata_snapshot,
            query_info.prewhere_info,
            true,
            column_names,
            MergeTreeReadPool::BackoffSettings(settings),
            settings.preferred_block_size_bytes,
            false);

        /// Let's estimate total number of rows for progress bar.
        LOG_TRACE(log, "Reading approx. {} rows with {} streams", total_rows, num_streams);

        for (size_t i = 0; i < num_streams; ++i)
        {
            auto source = std::make_shared<MergeTreeThreadSelectBlockInputProcessor>(
                i, pool, min_marks_for_concurrent_read, max_block_size,
                settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
                data, metadata_snapshot, use_uncompressed_cache,
                query_info.prewhere_info, reader_settings, virt_columns);

            if (i == 0)
            {
                /// Set the approximate number of rows for the first source only
                source->addTotalRowsApprox(total_rows);
            }

            res.emplace_back(std::move(source));
        }

        return createPlanFromPipe(Pipe::unitePipes(std::move(res)));
    }
    else
    {
        /// Sequential query execution.
        Pipes res;

        for (const auto & part : parts)
        {
            auto source = std::make_shared<MergeTreeSelectProcessor>(
                data, metadata_snapshot, part.data_part, max_block_size, settings.preferred_block_size_bytes,
                settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges, use_uncompressed_cache,
                query_info.prewhere_info, true, reader_settings, virt_columns, part.part_index_in_query);

            res.emplace_back(std::move(source));
        }

        auto pipe = Pipe::unitePipes(std::move(res));

        /// Use ConcatProcessor to concat sources together.
        /// It is needed to read in parts order (and so in PK order) if single thread is used.
        if (pipe.numOutputPorts() > 1)
            pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

        return createPlanFromPipe(std::move(pipe));
    }
}

static ActionsDAGPtr createProjection(const Block & header)
{
    auto projection = std::make_shared<ActionsDAG>(header.getNamesAndTypesList());
    projection->removeUnusedActions(header.getNames());
    projection->projectInput();
    return projection;
}

QueryPlanPtr MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreamsWithOrder(
    RangesInDataParts && parts,
    size_t num_streams,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 max_block_size,
    bool use_uncompressed_cache,
    const SelectQueryInfo & query_info,
    const ActionsDAGPtr & sorting_key_prefix_expr,
    const Names & virt_columns,
    const Settings & settings,
    const MergeTreeReaderSettings & reader_settings,
    ActionsDAGPtr & out_projection) const
{
    size_t sum_marks = 0;
    const InputOrderInfoPtr & input_order_info = query_info.input_order_info;

    size_t adaptive_parts = 0;
    std::vector<size_t> sum_marks_in_parts(parts.size());
    const auto data_settings = data.getSettings();

    for (size_t i = 0; i < parts.size(); ++i)
    {
        sum_marks_in_parts[i] = parts[i].getMarksCount();
        sum_marks += sum_marks_in_parts[i];

        if (parts[i].data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts > parts.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        settings.merge_tree_max_rows_to_use_cache,
        settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    const size_t min_marks_for_concurrent_read = roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_concurrent_read,
        settings.merge_tree_min_bytes_for_concurrent_read,
        data_settings->index_granularity,
        index_granularity_bytes);

    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    Pipes res;

    if (sum_marks == 0)
        return {};

    /// Let's split ranges to avoid reading much data.
    auto split_ranges = [rows_granularity = data_settings->index_granularity, max_block_size](const auto & ranges, int direction)
    {
        MarkRanges new_ranges;
        const size_t max_marks_in_range = (max_block_size + rows_granularity - 1) / rows_granularity;
        size_t marks_in_range = 1;

        if (direction == 1)
        {
            /// Split first few ranges to avoid reading much data.
            bool split = false;
            for (auto range : ranges)
            {
                while (!split && range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_back(range.begin, range.begin + marks_in_range);
                    range.begin += marks_in_range;
                    marks_in_range *= 2;

                    if (marks_in_range > max_marks_in_range)
                        split = true;
                }
                new_ranges.emplace_back(range.begin, range.end);
            }
        }
        else
        {
            /// Split all ranges to avoid reading much data, because we have to
            ///  store whole range in memory to reverse it.
            for (auto it = ranges.rbegin(); it != ranges.rend(); ++it)
            {
                auto range = *it;
                while (range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_front(range.end - marks_in_range, range.end);
                    range.end -= marks_in_range;
                    marks_in_range = std::min(marks_in_range * 2, max_marks_in_range);
                }
                new_ranges.emplace_front(range.begin, range.end);
            }
        }

        return new_ranges;
    };

    const size_t min_marks_per_stream = (sum_marks - 1) / num_streams + 1;
    bool need_preliminary_merge = (parts.size() > settings.read_in_order_two_level_merge_threshold);

    std::vector<QueryPlanPtr> plans;

    for (size_t i = 0; i < num_streams && !parts.empty(); ++i)
    {
        size_t need_marks = min_marks_per_stream;

        Pipes pipes;

        /// Loop over parts.
        /// We will iteratively take part or some subrange of a part from the back
        ///  and assign a stream to read from it.
        while (need_marks > 0 && !parts.empty())
        {
            RangesInDataPart part = parts.back();
            parts.pop_back();

            size_t & marks_in_part = sum_marks_in_parts.back();

            /// We will not take too few rows from a part.
            if (marks_in_part >= min_marks_for_concurrent_read &&
                need_marks < min_marks_for_concurrent_read)
                need_marks = min_marks_for_concurrent_read;

            /// Do not leave too few rows in the part.
            if (marks_in_part > need_marks &&
                marks_in_part - need_marks < min_marks_for_concurrent_read)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;

            /// We take the whole part if it is small enough.
            if (marks_in_part <= need_marks)
            {
                ranges_to_get_from_part = part.ranges;

                need_marks -= marks_in_part;
                sum_marks_in_parts.pop_back();
            }
            else
            {
                /// Loop through ranges in part. Take enough ranges to cover "need_marks".
                while (need_marks > 0)
                {
                    if (part.ranges.empty())
                        throw Exception("Unexpected end of ranges while spreading marks among streams", ErrorCodes::LOGICAL_ERROR);

                    MarkRange & range = part.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part.ranges.pop_front();
                }
                parts.emplace_back(part);
            }
            ranges_to_get_from_part = split_ranges(ranges_to_get_from_part, input_order_info->direction);

            if (input_order_info->direction == 1)
            {
                pipes.emplace_back(std::make_shared<MergeTreeSelectProcessor>(
                    data,
                    metadata_snapshot,
                    part.data_part,
                    max_block_size,
                    settings.preferred_block_size_bytes,
                    settings.preferred_max_column_in_block_size_bytes,
                    column_names,
                    ranges_to_get_from_part,
                    use_uncompressed_cache,
                    query_info.prewhere_info,
                    true,
                    reader_settings,
                    virt_columns,
                    part.part_index_in_query));
            }
            else
            {
                pipes.emplace_back(std::make_shared<MergeTreeReverseSelectProcessor>(
                    data,
                    metadata_snapshot,
                    part.data_part,
                    max_block_size,
                    settings.preferred_block_size_bytes,
                    settings.preferred_max_column_in_block_size_bytes,
                    column_names,
                    ranges_to_get_from_part,
                    use_uncompressed_cache,
                    query_info.prewhere_info,
                    true,
                    reader_settings,
                    virt_columns,
                    part.part_index_in_query));
            }
        }

        auto plan = createPlanFromPipe(Pipe::unitePipes(std::move(pipes)), " with order");

        if (input_order_info->direction != 1)
        {
            auto reverse_step = std::make_unique<ReverseRowsStep>(plan->getCurrentDataStream());
            plan->addStep(std::move(reverse_step));
        }

        plans.emplace_back(std::move(plan));
    }

    if (need_preliminary_merge)
    {
        SortDescription sort_description;
        for (size_t j = 0; j < input_order_info->order_key_prefix_descr.size(); ++j)
            sort_description.emplace_back(metadata_snapshot->getSortingKey().column_names[j],
                                          input_order_info->direction, 1);

        for (auto & plan : plans)
        {
            /// Drop temporary columns, added by 'sorting_key_prefix_expr'
            out_projection = createProjection(plan->getCurrentDataStream().header);

            auto expression_step = std::make_unique<ExpressionStep>(
                    plan->getCurrentDataStream(),
                    sorting_key_prefix_expr);

            expression_step->setStepDescription("Calculate sorting key prefix");
            plan->addStep(std::move(expression_step));

            auto merging_sorted = std::make_unique<MergingSortedStep>(
                    plan->getCurrentDataStream(),
                    sort_description,
                    max_block_size);

            merging_sorted->setStepDescription("Merge sorting mark ranges");
            plan->addStep(std::move(merging_sorted));
        }
    }

    if (plans.size() == 1)
        return std::move(plans.front());

    DataStreams input_streams;
    for (const auto & plan : plans)
        input_streams.emplace_back(plan->getCurrentDataStream());

    const auto & common_header = plans.front()->getCurrentDataStream().header;
    auto union_step = std::make_unique<UnionStep>(std::move(input_streams), common_header);

    auto plan = std::make_unique<QueryPlan>();
    plan->unitePlans(std::move(union_step), std::move(plans));

    return plan;
}


QueryPlanPtr MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreamsFinal(
    RangesInDataParts && parts,
    size_t num_streams,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 max_block_size,
    bool use_uncompressed_cache,
    const SelectQueryInfo & query_info,
    const Names & virt_columns,
    const Settings & settings,
    const MergeTreeReaderSettings & reader_settings,
    ActionsDAGPtr & out_projection) const
{
    const auto data_settings = data.getSettings();
    size_t sum_marks = 0;
    size_t adaptive_parts = 0;
    for (const auto & part : parts)
    {
        for (const auto & range : part.ranges)
            sum_marks += range.end - range.begin;

        if (part.data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts >= parts.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        settings.merge_tree_max_rows_to_use_cache,
        settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    if (num_streams > settings.max_final_threads)
        num_streams = settings.max_final_threads;

    /// If setting do_not_merge_across_partitions_select_final is true than we won't merge parts from different partitions.
    /// We have all parts in parts vector, where parts with same partition are nerby.
    /// So we will store iterators pointed to the beginning of each partition range (and parts.end()),
    /// then we will create a pipe for each partition that will run selecting processor and merging processor
    /// for the parts with this partition. In the end we will unite all the pipes.
    std::vector<RangesInDataParts::iterator> parts_to_merge_ranges;
    auto it = parts.begin();
    parts_to_merge_ranges.push_back(it);

    if (settings.do_not_merge_across_partitions_select_final)
    {
        while (it != parts.end())
        {
            it = std::find_if(
                it, parts.end(), [&it](auto & part) { return it->data_part->info.partition_id != part.data_part->info.partition_id; });
            parts_to_merge_ranges.push_back(it);
        }
        /// We divide threads for each partition equally. But we will create at least the number of partitions threads.
        /// (So, the total number of threads could be more than initial num_streams.
        num_streams /= (parts_to_merge_ranges.size() - 1);
    }
    else
    {
        /// If do_not_merge_across_partitions_select_final is false we just merge all the parts.
        parts_to_merge_ranges.push_back(parts.end());
    }

    std::vector<QueryPlanPtr> partition_plans;

    for (size_t range_index = 0; range_index < parts_to_merge_ranges.size() - 1; ++range_index)
    {
        QueryPlanPtr plan;

        {
            Pipes pipes;

            for (auto part_it = parts_to_merge_ranges[range_index]; part_it != parts_to_merge_ranges[range_index + 1]; ++part_it)
            {
                auto source_processor = std::make_shared<MergeTreeSelectProcessor>(
                    data,
                    metadata_snapshot,
                    part_it->data_part,
                    max_block_size,
                    settings.preferred_block_size_bytes,
                    settings.preferred_max_column_in_block_size_bytes,
                    column_names,
                    part_it->ranges,
                    use_uncompressed_cache,
                    query_info.prewhere_info,
                    true,
                    reader_settings,
                    virt_columns,
                    part_it->part_index_in_query);

                pipes.emplace_back(std::move(source_processor));
            }

            if (pipes.empty())
                continue;

            auto pipe = Pipe::unitePipes(std::move(pipes));

            /// Drop temporary columns, added by 'sorting_key_expr'
            if (!out_projection)
                out_projection = createProjection(pipe.getHeader());

            plan = createPlanFromPipe(std::move(pipe), "with final");
        }

        /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
        /// with level > 0 then we won't postprocess this part
        if (settings.do_not_merge_across_partitions_select_final &&
            std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
            parts_to_merge_ranges[range_index]->data_part->info.level > 0)
        {
            partition_plans.emplace_back(std::move(plan));
            continue;
        }

        auto expression_step = std::make_unique<ExpressionStep>(
                plan->getCurrentDataStream(),
                metadata_snapshot->getSortingKey().expression->getActionsDAG().clone());

        expression_step->setStepDescription("Calculate sorting key expression");
        plan->addStep(std::move(expression_step));

        Names sort_columns = metadata_snapshot->getSortingKeyColumns();
        SortDescription sort_description;
        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

        const auto & header = plan->getCurrentDataStream().header;
        for (size_t i = 0; i < sort_columns_size; ++i)
            sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);

        auto final_step = std::make_unique<MergingFinal>(
            plan->getCurrentDataStream(),
            std::min<size_t>(num_streams, settings.max_final_threads),
            sort_description,
            data.merging_params,
            partition_key_columns,
            max_block_size);

        final_step->setStepDescription("Merge rows for FINAL");
        plan->addStep(std::move(final_step));

        partition_plans.emplace_back(std::move(plan));
    }

    if (partition_plans.empty())
        return {};

    if (partition_plans.size() == 1)
        return std::move(partition_plans.front());

    auto result_header = partition_plans.front()->getCurrentDataStream().header;
    DataStreams input_streams;
    for (const auto & partition_plan : partition_plans)
        input_streams.push_back(partition_plan->getCurrentDataStream());

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams), result_header);
    union_step->setStepDescription("Unite sources after FINAL");
    QueryPlanPtr plan = std::make_unique<QueryPlan>();
    plan->unitePlans(std::move(union_step), std::move(partition_plans));
    return plan;
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
        LOG_TRACE(log, "Not using primary index on part {}", part->name);

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

    size_t granules_dropped = 0;
    size_t total_granules = 0;

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

    LOG_DEBUG(log, "Index {} has dropped {} / {} granules.", backQuote(index_helper->index.name), granules_dropped, total_granules);

    return res;
}


}
