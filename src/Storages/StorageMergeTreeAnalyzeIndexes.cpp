#include <unordered_set>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTreeAnalyzeIndexes.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Access/Common/AccessFlags.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

///
/// MergeTreeAnalyzeIndexSource
///
class MergeTreeAnalyzeIndexSource final : public ISource, WithContext
{
public:
    MergeTreeAnalyzeIndexSource(
        SharedHeader header_,
        std::vector<UInt8> columns_mask_,
        const StoragePtr & storage_,
        const SelectQueryInfo & query_info_,
        size_t num_streams_,
        MergeTreeData::DataPartsVector data_parts_,
        MergeTreeSettingsPtr table_settings_,
        const ASTPtr & predicate_,
        const OptionalVectorSearchParameters & vector_search_parameters_,
        std::unordered_map<String, MarkRanges> requested_ranges_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , header(std::move(header_))
        , columns_mask(std::move(columns_mask_))
        , storage(storage_)
        , query_info(query_info_)
        , num_streams(num_streams_)
        , predicate(predicate_)
        , vector_search_parameters(vector_search_parameters_)
        , data_parts(std::move(data_parts_))
        , table_settings(std::move(table_settings_))
        , requested_ranges(std::move(requested_ranges_))
    {
    }

    String getName() const override { return "MergeTreeAnalyzeIndexes"; }

protected:
    Chunk generate() override
    {
        if (std::exchange(analyzed, true))
            return {};

        if (data_parts.empty())
            return {};

        auto component_guard = Coordination::setCurrentComponent("MergeTreeAnalyzeIndexSource::generate");

        auto ranges = getIndexAnalysis();
        MutableColumns res_columns = header->cloneEmptyColumns();

        std::unordered_set<std::string> processed_parts;

        for (const auto & ranges_in_part : ranges)
        {
            size_t src_index = 0;
            size_t res_index = 0;
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(ranges_in_part.data_part->name);

            /// ranges
            if (columns_mask[src_index++])
            {
                Array field;
                for (const auto & range : ranges_in_part.ranges)
                    field.push_back(Tuple{range.begin, range.end});
                res_columns[res_index++]->insert(std::move(field));
            }

            processed_parts.insert(ranges_in_part.data_part->name);
        }

        /// Add existing parts, but filtered out into the result.
        for (const auto & part : data_parts)
        {
            if (processed_parts.contains(part->name))
                continue;

            size_t src_index = 0;
            size_t res_index = 0;
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(part->name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insertDefault();
        }

        size_t rows = res_columns.front()->size();
        return Chunk(std::move(res_columns), rows);
    }

    RangesInDataParts getIndexAnalysis()
    {
        const auto & context = getContext();

        auto reader_settings = MergeTreeReaderSettings::createForQuery(context, *table_settings, query_info);

        const auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
        const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(storage.get());
        if (!merge_tree_data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeAnalyzeIndexes expected MergeTree table, got: {}", storage->getName());

        std::optional<ActionsDAG> filter_dag;
        if (predicate)
        {
            auto execution_context = Context::createCopy(context);
            execution_context->setSetting("enable_parallel_blocks_marshalling", false);

            auto expression = buildQueryTree(predicate, execution_context);

            auto dummy_storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, metadata_snapshot->getColumns());
            QueryTreeNodePtr fake_table_expression = std::make_shared<TableNode>(dummy_storage, execution_context);

            QueryAnalyzer analyzer(false);
            analyzer.resolveConstantExpression(expression, fake_table_expression, execution_context);

            GlobalPlannerContextPtr global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
            auto planner_context = std::make_shared<PlannerContext>(execution_context, global_planner_context, SelectQueryOptions{});

            collectSourceColumns(expression, planner_context, /*keep_alias_columns=*/ false);
            collectSets(expression, *planner_context);

            ColumnNodePtrWithHashSet empty_correlated_columns_set;
            auto [actions, correlated_subtrees] = buildActionsDAGFromExpressionNode(
                expression,
                /*input_columns=*/ {},
                planner_context,
                empty_correlated_columns_set);
            correlated_subtrees.assertEmpty("in constant expression without query context");

            auto subquery_options = SelectQueryOptions{}.subquery();
            subquery_options.forceMaterializeCTE();
            subquery_options.ignore_limits = false;
            for (auto & subquery : planner_context->getPreparedSets().getSubqueries())
            {
                auto query_tree = subquery->detachQueryTree();
                createUniqueAliasesIfNecessary(query_tree, execution_context);
                Planner subquery_planner(
                    query_tree,
                    subquery_options,
                    std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}));
                subquery_planner.buildQueryPlanIfNeeded();

                auto subquery_plan = std::move(subquery_planner).extractQueryPlan();
                subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_plan)));
            }

            filter_dag.emplace(std::move(actions));
        }

        RangesInDataParts parts_ranges{data_parts};
        for (auto & part_ranges : parts_ranges)
        {
            auto it = requested_ranges.find(part_ranges.data_part->name);
            if (it == requested_ranges.end())
                continue;

            /// Ranges are sorted at parse time, so checking the last end suffices. An out-of-bounds
            /// analysis range would dereference the primary index outside the loaded window.
            size_t marks_count = part_ranges.data_part->index_granularity->getMarksCountWithoutFinal();
            if (!it->second.empty() && it->second.back().end > marks_count)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Mark range ({}, {}) exceeds {} marks of part {}",
                    it->second.back().begin, it->second.back().end, marks_count, part_ranges.data_part->name);

            part_ranges.ranges = it->second;
        }

        const StorageSnapshotPtr storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);
        const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);

        std::optional<ReadFromMergeTree::Indexes> indexes;
        ReadFromMergeTree::buildIndexes(
            indexes,
            filter_dag ? &filter_dag.value() : nullptr,
            *merge_tree_data,
            parts_ranges,
            vector_search_parameters,
            /*top_k_filter_info=*/ std::nullopt,
            context,
            query_info,
            metadata_snapshot);

        /// TODO: we may also want to support query condition cache here as well

        ReadFromMergeTree::AnalysisResult analysis_result;
        indexes->use_skip_indexes_on_data_read = false; /// for static skip index analysis
        indexes->use_skip_indexes_if_final_exact_mode = false; /// not supported in distributed index analysis
        MergeTreeDataSelectExecutor::IndexAnalysisContext filter_context
        {
            .metadata_snapshot = metadata_snapshot,
            .mutations_snapshot = snapshot_data.mutations_snapshot,
            .query_info = query_info,
            .context = context,
            .indexes = *indexes,
            .top_k_filter_info = std::nullopt,
            .reader_settings = reader_settings,
            .log = getLogger("MergeTreeAnalyzeIndexSource"),
            .num_streams = num_streams,
            .find_exact_ranges = false,
            .is_parallel_reading_from_replicas = false,
            .has_projections = false,
            .load_partial_primary_key = !requested_ranges.empty(),
            .result = analysis_result,
        };
        return MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(filter_context, parts_ranges, analysis_result.index_stats);
    }

private:
    SharedHeader header;
    std::vector<UInt8> columns_mask;
    const StoragePtr storage;
    SelectQueryInfo query_info;
    size_t num_streams;
    ASTPtr predicate;
    OptionalVectorSearchParameters vector_search_parameters;
    MergeTreeData::DataPartsVector data_parts;
    MergeTreeSettingsPtr table_settings;
    std::unordered_map<String, MarkRanges> requested_ranges;

    bool analyzed = false;
};

///
/// ReadFromMergeTreeAnalyzeIndex
///
class ReadFromMergeTreeAnalyzeIndexes : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromMergeTreeAnalyzeIndexes"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromMergeTreeAnalyzeIndexes(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        size_t num_streams_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        std::vector<UInt8> columns_mask_,
        std::shared_ptr<StorageMergeTreeAnalyzeIndexes> storage_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , columns_mask(std::move(columns_mask_))
        , storage(std::move(storage_))
        , num_streams(num_streams_)
        , log(&Poco::Logger::get("StorageMergeTreeAnalyzeIndexes"))
    {
    }

private:
    std::vector<UInt8> columns_mask;
    std::shared_ptr<StorageMergeTreeAnalyzeIndexes> storage;
    const size_t num_streams;
    Poco::Logger * log;
};

void ReadFromMergeTreeAnalyzeIndexes::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    LOG_DEBUG(log, "Analyzing index from {} parts of table {}",
        storage->data_parts.size(),
        storage->source_table->getStorageID().getNameForLogs());

    pipeline.init(Pipe(std::make_shared<MergeTreeAnalyzeIndexSource>(
        getOutputHeader(),
        columns_mask,
        storage->source_table,
        getQueryInfo(),
        num_streams,
        storage->data_parts,
        storage->table_settings,
        storage->predicate,
        storage->vector_search_parameters,
        storage->requested_ranges,
        context)));
}


///
/// StorageMergeTreeAnalyzeIndex
///
StorageMergeTreeAnalyzeIndexes::StorageMergeTreeAnalyzeIndexes(
    const StorageID & table_id_,
    const StoragePtr & source_table_,
    const ColumnsDescription & columns,
    std::vector<String> parts_,
    VectorWithMemoryTracking<MarkRanges> parts_ranges_,
    const ASTPtr & predicate_,
    const OptionalVectorSearchParameters & vector_search_parameters_)
    : StorageWithCommonVirtualColumns(table_id_)
    , source_table(source_table_)
    , predicate(predicate_)
    , vector_search_parameters(vector_search_parameters_)
{
    const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree_data)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeAnalyzeIndexes expected MergeTree table, got: {}", source_table->getName());

    data_parts = merge_tree_data->getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });
    if (!parts_.empty())
    {
        chassert(parts_.size() == parts_ranges_.size());
        std::unordered_set<String> parts_set;
        parts_set.reserve(parts_.size());
        for (size_t i = 0; i < parts_.size(); ++i)
        {
            const bool with_ranges = !parts_ranges_[i].empty();
            const bool duplicate = !parts_set.insert(parts_[i]).second;
            /// Repeating a part with explicit ranges is ambiguous and would otherwise silently
            /// drop all but the first entry; bare duplicates keep the historical silent dedup.
            if (duplicate && (with_ranges || requested_ranges.contains(parts_[i])))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate part {} in the parts argument", parts_[i]);
            if (with_ranges)
                requested_ranges.emplace(parts_[i], std::move(parts_ranges_[i]));
        }
        std::erase_if(data_parts, [&](const MergeTreeData::DataPartPtr & part) { return !parts_set.contains(part->name); });
    }

    table_settings = merge_tree_data->getSettings();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageMergeTreeAnalyzeIndexes::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

void StorageMergeTreeAnalyzeIndexes::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t /*max_block_size*/,
    size_t num_streams)
{
    context->checkAccess(AccessType::SELECT, source_table->getStorageID());

    auto sample = storage_snapshot->metadata->getSampleBlock();
    auto [columns_mask, header] = getQueriedColumnsMaskAndHeader(sample, column_names);
    auto this_ptr = std::static_pointer_cast<StorageMergeTreeAnalyzeIndexes>(shared_from_this());

    auto reading = std::make_unique<ReadFromMergeTreeAnalyzeIndexes>(
        column_names,
        query_info,
        num_streams,
        storage_snapshot,
        std::move(context),
        std::make_shared<Block>(header),
        std::move(columns_mask),
        std::move(this_ptr));

    query_plan.addStep(std::move(reading));
}


}
