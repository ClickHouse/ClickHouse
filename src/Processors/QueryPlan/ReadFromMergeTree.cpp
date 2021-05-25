#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/VirtualColumnUtils.h>
#include <common/logger_useful.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INDEX_NOT_USED;
}

ReadFromMergeTree::ReadFromMergeTree(
    SelectQueryInfo query_info_,
    const MergeTreeDataSelectExecutor::PartitionIdToMaxBlock * max_block_numbers_to_read_,
    ContextPtr context_,
    const MergeTreeData & data_,
    const MergeTreeData & storage_,
    StorageMetadataPtr metadata_snapshot_,
    String query_id_,
    Names real_column_names_,
    MergeTreeData::DataPartsVector parts_,
    //IndexStatPtr index_stats_,
    PrewhereInfoPtr prewhere_info_,
    Names virt_column_names_,
    Settings settings_,
    size_t num_streams_,
    ReadType read_type_)
    : ISourceStep(DataStream{.header = MergeTreeBaseSelectProcessor::transformHeader(
        metadata_snapshot_->getSampleBlockForColumns(real_column_names_, storage_.getVirtuals(), storage_.getStorageID()),
        prewhere_info_,
        storage_.getPartitionValueType(),
        virt_column_names_)})
    , query_info(std::move(query_info_))
    , max_block_numbers_to_read(max_block_numbers_to_read_)
    , context(std::move(context_))
    , data(data_)
    , storage(storage_)
    , metadata_snapshot(std::move(metadata_snapshot_))
    , query_id(std::move(query_id_))
    , real_column_names(std::move(real_column_names_))
    , parts(std::move(parts_))
    //, index_stats(std::move(index_stats_))
    , prewhere_info(std::move(prewhere_info_))
    , virt_column_names(std::move(virt_column_names_))
    , settings(std::move(settings_))
    , num_streams(num_streams_)
    , read_type(read_type_)
{
}

Pipe ReadFromMergeTree::readFromPool()
{
    Pipes pipes;
    size_t sum_marks = 0;
    size_t total_rows = 0;

    for (const auto & part : parts)
    {
        sum_marks += part.getMarksCount();
        total_rows += part.getRowsCount();
    }

    auto pool = std::make_shared<MergeTreeReadPool>(
        num_streams,
        sum_marks,
        settings.min_marks_for_concurrent_read,
        std::move(parts),
        storage,
        metadata_snapshot,
        prewhere_info,
        true,
        required_columns,
        settings.backoff_settings,
        settings.preferred_block_size_bytes,
        false);

    auto * logger = &Poco::Logger::get(storage.getLogName() + " (SelectExecutor)");
    LOG_DEBUG(logger, "Reading approx. {} rows with {} streams", total_rows, num_streams);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<MergeTreeThreadSelectBlockInputProcessor>(
            i, pool, settings.min_marks_for_concurrent_read, settings.max_block_size,
            settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
            storage, metadata_snapshot, settings.use_uncompressed_cache,
            prewhere_info, settings.reader_settings, virt_column_names);

        if (i == 0)
        {
            /// Set the approximate number of rows for the first source only
            source->addTotalRowsApprox(total_rows);
        }

        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}

template<typename TSource>
ProcessorPtr ReadFromMergeTree::createSource(const RangesInDataPart & part)
{
    return std::make_shared<TSource>(
            storage, metadata_snapshot, part.data_part, settings.max_block_size, settings.preferred_block_size_bytes,
            settings.preferred_max_column_in_block_size_bytes, required_columns, part.ranges, settings.use_uncompressed_cache,
            prewhere_info, true, settings.reader_settings, virt_column_names, part.part_index_in_query);
}

Pipe ReadFromMergeTree::readInOrder()
{
    Pipes pipes;
    for (const auto & part : parts)
    {
        auto source = read_type == ReadType::InReverseOrder
                    ? createSource<MergeTreeReverseSelectProcessor>(part)
                    : createSource<MergeTreeSelectProcessor>(part);

        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    if (read_type == ReadType::InReverseOrder)
    {
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ReverseTransform>(header);
        });
    }

    return pipe;
}

Pipe ReadFromMergeTree::read()
{
    if (read_type == ReadType::Default && num_streams > 1)
        return readFromPool();

    auto pipe = readInOrder();

    /// Use ConcatProcessor to concat sources together.
    /// It is needed to read in parts order (and so in PK order) if single thread is used.
    if (read_type == ReadType::Default && pipe.numOutputPorts() > 1)
        pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

    return pipe;
}

static std::optional<std::unordered_set<String>> filterPartsByVirtualColumns(
    const MergeTreeData & data,
    MergeTreeData::DataPartsVector & parts,
    ASTPtr & query,
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

static void filterPartsByPartition(
    StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData & data,
    SelectQueryInfo & query_info,
    ContextPtr & context,
    ContextPtr & query_context,
    MergeTreeData::DataPartsVector & parts,
    const std::optional<std::unordered_set<String>> & part_values,
    const MergeTreeDataSelectExecutor::PartitionIdToMaxBlock * max_block_numbers_to_read,
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
        partition_pruner.emplace(metadata_snapshot->getPartitionKey(), query_info, context, false /* strict */);

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

    MergeTreeDataSelectExecutor::PartFilterCounters part_filter_counters;
    if (query_context->getSettingsRef().allow_experimental_query_deduplication)
        MergeTreeDataSelectExecutor::selectPartsToReadWithUUIDFilter(
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
        MergeTreeDataSelectExecutor::selectPartsToRead(
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

void ReadFromMergeTree::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    auto part_values = filterPartsByVirtualColumns(data, parts, query_info.query, context);
    if (part_values && part_values->empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (real_column_names.empty())
    {
        NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
        real_column_names.push_back(ExpressionActions::getSmallestColumn(available_real_columns));
    }

    metadata_snapshot->check(real_column_names, data.getVirtuals(), data.getStorageID());

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    Names primary_key_columns = primary_key.column_names;
    KeyCondition key_condition(query_info, context, primary_key_columns, primary_key.expression);

    if (settings.force_primary_key && key_condition.alwaysUnknownOrTrue())
    {
        throw Exception(
            ErrorCodes::INDEX_NOT_USED,
            "Primary key ({}) is not used and setting 'force_primary_key' is set.",
            fmt::join(primary_key_columns, ", "));
    }
    LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    auto query_context = context->hasQueryContext() ? context->getQueryContext() : context;

    filterPartsByPartition(
        metadata_snapshot_base, data, query_info, context, query_context, parts, part_values, max_block_numbers_to_read, log, index_stats);

    bool sample_factor_column_queried = false;
    for (const auto & col : virt_column_names)
        if (col == "_sample_factor")
            sample_factor_column_queried = true;

    auto sampling = MergeTreeDataSelectExecutor::getSampling(
        select, parts, metadata_snapshot, key_condition, settings, data, log, sample_factor_column_queried, metadata_snapshot->getColumns().getAllPhysical(), context)

    Pipe pipe = read();

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    // Attach QueryIdHolder if needed
    if (!query_id.empty())
        pipe.addQueryIdHolder(std::make_shared<QueryIdHolder>(query_id, storage));

    pipeline.init(std::move(pipe));
}

static const char * indexTypeToString(ReadFromMergeTree::IndexType type)
{
    switch (type)
    {
        case ReadFromMergeTree::IndexType::None:
            return "None";
        case ReadFromMergeTree::IndexType::MinMax:
            return "MinMax";
        case ReadFromMergeTree::IndexType::Partition:
            return "Partition";
        case ReadFromMergeTree::IndexType::PrimaryKey:
            return "PrimaryKey";
        case ReadFromMergeTree::IndexType::Skip:
            return "Skip";
    }

    __builtin_unreachable();
}

static const char * readTypeToString(ReadFromMergeTree::ReadType type)
{
    switch (type)
    {
        case ReadFromMergeTree::ReadType::Default:
            return "Default";
        case ReadFromMergeTree::ReadType::InOrder:
            return "InOrder";
        case ReadFromMergeTree::ReadType::InReverseOrder:
            return "InReverseOrder";
    }

    __builtin_unreachable();
}

void ReadFromMergeTree::describeActions(FormatSettings & format_settings) const
{
    std::string prefix(format_settings.offset, format_settings.indent_char);
    format_settings.out << prefix << "ReadType: " << readTypeToString(read_type) << '\n';

    if (index_stats && !index_stats->empty())
    {
        format_settings.out << prefix << "Parts: " << index_stats->back().num_parts_after << '\n';
        format_settings.out << prefix << "Granules: " << index_stats->back().num_granules_after << '\n';
    }
}

void ReadFromMergeTree::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Read Type", readTypeToString(read_type));
    if (index_stats && !index_stats->empty())
    {
        map.add("Parts", index_stats->back().num_parts_after);
        map.add("Granules", index_stats->back().num_granules_after);
    }
}

void ReadFromMergeTree::describeIndexes(FormatSettings & format_settings) const
{
    std::string prefix(format_settings.offset, format_settings.indent_char);
    if (index_stats && !index_stats->empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats->size() == 1 && index_stats->front().type == IndexType::None)
            return;

        std::string indent(format_settings.indent, format_settings.indent_char);
        format_settings.out << prefix << "Indexes:\n";

        for (size_t i = 0; i < index_stats->size(); ++i)
        {
            const auto & stat = (*index_stats)[i];
            if (stat.type == IndexType::None)
                continue;

            format_settings.out << prefix << indent << indexTypeToString(stat.type) << '\n';

            if (!stat.name.empty())
                format_settings.out << prefix << indent << indent << "Name: " << stat.name << '\n';

            if (!stat.description.empty())
                format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

            if (!stat.used_keys.empty())
            {
                format_settings.out << prefix << indent << indent << "Keys: " << stat.name << '\n';
                for (const auto & used_key : stat.used_keys)
                    format_settings.out << prefix << indent << indent << indent << used_key << '\n';
            }

            if (!stat.condition.empty())
                format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

            format_settings.out << prefix << indent << indent << "Parts: " << stat.num_parts_after;
            if (i)
                format_settings.out << '/' << (*index_stats)[i - 1].num_parts_after;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Granules: " << stat.num_granules_after;
            if (i)
                format_settings.out << '/' << (*index_stats)[i - 1].num_granules_after;
            format_settings.out << '\n';
        }
    }
}

void ReadFromMergeTree::describeIndexes(JSONBuilder::JSONMap & map) const
{
    if (index_stats && !index_stats->empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats->size() == 1 && index_stats->front().type == IndexType::None)
            return;

        auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();

        for (size_t i = 0; i < index_stats->size(); ++i)
        {
            const auto & stat = (*index_stats)[i];
            if (stat.type == IndexType::None)
                continue;

            auto index_map = std::make_unique<JSONBuilder::JSONMap>();

            index_map->add("Type", indexTypeToString(stat.type));

            if (!stat.name.empty())
                index_map->add("Name", stat.name);

            if (!stat.description.empty())
                index_map->add("Description", stat.description);

            if (!stat.used_keys.empty())
            {
                auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

                for (const auto & used_key : stat.used_keys)
                    keys_array->add(used_key);

                index_map->add("Keys", std::move(keys_array));
            }

            if (!stat.condition.empty())
                index_map->add("Condition", stat.condition);

            if (i)
                index_map->add("Initial Parts", (*index_stats)[i - 1].num_parts_after);
            index_map->add("Selected Parts", stat.num_parts_after);

            if (i)
                index_map->add("Initial Granules", (*index_stats)[i - 1].num_granules_after);
            index_map->add("Selected Granules", stat.num_granules_after);

            indexes_array->add(std::move(index_map));
        }

        map.add("Indexes", std::move(indexes_array));
    }
}

}
