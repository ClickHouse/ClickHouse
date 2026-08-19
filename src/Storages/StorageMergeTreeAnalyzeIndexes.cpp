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
        StorageMetadataPtr metadata_snapshot_,
        const SelectQueryInfo & query_info_,
        size_t num_streams_,
        RangesInDataParts analysis_parts_,
        String projection_name_,
        MergeTreeSettingsPtr table_settings_,
        const ASTPtr & predicate_,
        const OptionalVectorSearchParameters & vector_search_parameters_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , header(std::move(header_))
        , columns_mask(std::move(columns_mask_))
        , storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , query_info(query_info_)
        , num_streams(num_streams_)
        , predicate(predicate_)
        , vector_search_parameters(vector_search_parameters_)
        , analysis_parts(std::move(analysis_parts_))
        , projection_name(std::move(projection_name_))
        , table_settings(std::move(table_settings_))
    {
    }

    String getName() const override { return "MergeTreeAnalyzeIndexes"; }

protected:
    Chunk generate() override
    {
        if (std::exchange(analyzed, true))
            return {};

        if (analysis_parts.empty())
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
                res_columns[res_index++]->insert(ranges_in_part.getAnalysisPartName());

            /// ranges
            if (columns_mask[src_index++])
            {
                Array field;
                for (const auto & range : ranges_in_part.ranges)
                    field.push_back(Tuple{range.begin, range.end});
                res_columns[res_index++]->insert(std::move(field));
            }

            processed_parts.insert(ranges_in_part.getAnalysisPartName());
        }

        /// Add existing parts, but filtered out into the result.
        for (const auto & part : analysis_parts)
        {
            const auto & part_name = part.getAnalysisPartName();
            if (processed_parts.contains(part_name))
                continue;

            size_t src_index = 0;
            size_t res_index = 0;
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(part_name);
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

        const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(storage.get());
        if (!merge_tree_data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeAnalyzeIndexes expected MergeTree table, got: {}", storage->getName());

        /// Projection parts are analyzed with the projection's own metadata (columns, primary key,
        /// skip indexes), the parent table provides only the parts and the settings.
        StorageMetadataPtr analysis_metadata_snapshot = metadata_snapshot;
        if (!projection_name.empty())
            analysis_metadata_snapshot = metadata_snapshot->projections.get(projection_name).metadata;

        std::optional<ActionsDAG> filter_dag;
        if (predicate)
        {
            auto execution_context = Context::createCopy(context);
            execution_context->setSetting("enable_parallel_blocks_marshalling", false);

            auto expression = buildQueryTree(predicate, execution_context);

            auto dummy_storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, analysis_metadata_snapshot->getColumns());
            auto fake_table_expression = std::make_shared<TableNode>(dummy_storage, execution_context);

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

        const StorageSnapshotPtr storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);
        const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);

        /// The initiator analyzes projection parts with an empty mutations snapshot, do the same.
        auto mutations_snapshot = snapshot_data.mutations_snapshot;
        if (!projection_name.empty())
            mutations_snapshot = mutations_snapshot->cloneEmpty();

        std::optional<ReadFromMergeTree::Indexes> indexes;
        ReadFromMergeTree::buildIndexes(
            indexes,
            filter_dag ? &filter_dag.value() : nullptr,
            *merge_tree_data,
            analysis_parts,
            vector_search_parameters,
            /*top_k_filter_info=*/ std::nullopt,
            context,
            query_info,
            analysis_metadata_snapshot);

        /// TODO: we may also want to support query condition cache here as well

        ReadFromMergeTree::AnalysisResult analysis_result;
        indexes->use_skip_indexes_on_data_read = false; /// for static skip index analysis
        indexes->use_skip_indexes_if_final_exact_mode = false; /// not supported in distributed index analysis
        MergeTreeDataSelectExecutor::IndexAnalysisContext filter_context
        {
            .metadata_snapshot = analysis_metadata_snapshot,
            .mutations_snapshot = mutations_snapshot,
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
            .check_row_limits = true,
            .result = analysis_result,
        };
        return MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(filter_context, analysis_parts, analysis_result.index_stats);
    }

private:
    SharedHeader header;
    std::vector<UInt8> columns_mask;
    const StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;
    SelectQueryInfo query_info;
    size_t num_streams;
    ASTPtr predicate;
    OptionalVectorSearchParameters vector_search_parameters;
    RangesInDataParts analysis_parts;
    String projection_name;
    MergeTreeSettingsPtr table_settings;

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
        storage->analysis_parts.size(),
        storage->source_table->getStorageID().getNameForLogs());

    pipeline.init(Pipe(std::make_shared<MergeTreeAnalyzeIndexSource>(
        getOutputHeader(),
        columns_mask,
        storage->source_table,
        storage->source_metadata_snapshot,
        getQueryInfo(),
        num_streams,
        storage->analysis_parts,
        storage->projection_name,
        storage->table_settings,
        storage->predicate,
        storage->vector_search_parameters,
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
    String projection_name_,
    const ASTPtr & predicate_,
    const OptionalVectorSearchParameters & vector_search_parameters_,
    ContextPtr context)
    : StorageWithCommonVirtualColumns(table_id_)
    , source_table(source_table_)
    , source_metadata_snapshot(source_table->getInMemoryMetadataPtr(context, false))
    , projection_name(std::move(projection_name_))
    , predicate(predicate_)
    , vector_search_parameters(vector_search_parameters_)
{
    const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree_data)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeAnalyzeIndexes expected MergeTree table, got: {}", source_table->getName());

    auto data_parts = merge_tree_data->getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });
    if (!parts_.empty())
    {
        std::unordered_set<String> parts_set(std::make_move_iterator(parts_.begin()), std::make_move_iterator(parts_.end()));
        std::erase_if(data_parts, [&](const MergeTreeData::DataPartPtr & part) { return !parts_set.contains(part->name); });
    }

    if (projection_name.empty())
    {
        analysis_parts = RangesInDataParts{data_parts};
    }
    else
    {
        if (!source_metadata_snapshot->projections.has(projection_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no projection {} in table {}",
                projection_name, source_table->getStorageID().getNameForLogs());

        analysis_parts.reserve(data_parts.size());
        size_t starting_offset = 0;
        for (const auto & parent_part : data_parts)
        {
            const auto & created_projections = parent_part->getProjectionParts();
            auto it = created_projections.find(projection_name);
            if (it != created_projections.end() && !it->second->is_broken)
            {
                analysis_parts.emplace_back(
                    /*data_part*/ it->second,
                    /*parent_part*/ parent_part,
                    /*part_index_in_query*/ analysis_parts.size(),
                    /*part_starting_offset_in_query*/ starting_offset);
            }
            /// Projection reads keep the parent numbering, so must the analysis
            /// (the projection may not preserve the row count of the parent).
            starting_offset += parent_part->rows_count;
        }
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
