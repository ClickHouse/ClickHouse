#include <unordered_set>
#include <Core/Field.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTreeAnalyzeIndexes.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/ExpressionActions.h>
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
    extern const int LOGICAL_ERROR;
}

///
/// MergeTreeAnalyzeIndexSource
///
class MergeTreeAnalyzeIndexSource : public ISource, WithContext
{
public:
    MergeTreeAnalyzeIndexSource(
        SharedHeader header_,
        const StoragePtr & storage_,
        const SelectQueryInfo & query_info_,
        size_t num_streams_,
        MergeTreeData::DataPartsVector data_parts_,
        MergeTreeSettingsPtr table_settings_,
        const ASTPtr & predicate_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , header(std::move(header_))
        , storage(storage_)
        , query_info(query_info_)
        , num_streams(num_streams_)
        , predicate(predicate_)
        , data_parts(std::move(data_parts_))
        , table_settings(std::move(table_settings_))
    {
    }

    String getName() const override { return "MergeTreeAnalyzeIndexes"; }

protected:
    Chunk generate() override
    {
        if (std::exchange(analyzed, true))
            return {};

        auto ranges = getIndexAnalysis();
        MutableColumns res_columns = header->cloneEmptyColumns();

        std::unordered_set<std::string> processed_parts;

        for (const auto & ranges_in_part : ranges)
        {
            size_t i = 0;
            res_columns[i++]->insert(ranges_in_part.data_part->name);

            /// ranges
            {
                Array field;
                for (const auto & range : ranges_in_part.ranges)
                    field.push_back(Tuple{range.begin, range.end});
                res_columns[i++]->insert(std::move(field));
            }

            processed_parts.insert(ranges_in_part.data_part->name);
        }

        /// Add existing parts, but filtered out into the result.
        for (const auto & part : data_parts)
        {
            if (processed_parts.contains(part->name))
                continue;

            size_t i = 0;
            res_columns[i++]->insert(part->name);
            res_columns[i++]->insertDefault();
        }

        size_t rows = res_columns.front()->size();
        return Chunk(std::move(res_columns), rows);
    }

    RangesInDataParts getIndexAnalysis()
    {
        const auto & context = getContext();

        ReadFromMergeTree::IndexStats index_stats;
        auto reader_settings = MergeTreeReaderSettings::create(context, *table_settings, query_info);

        StorageMetadataPtr metadata_snapshot = storage->getInMemoryMetadataPtr();
        const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(storage.get());
        if (!merge_tree_data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeAnalyzeIndexes expected MergeTree table, got: {}", storage->getName());

        std::optional<ActionsDAG> filter_dag;
        if (predicate)
        {
            /// FIXME:
            /// - use analyzer
            /// - use primary_key->getSampleBlock().getNamesAndTypesList() over metadata_snapshot->getSampleBlock().getNamesAndTypesList()
            TreeRewriterResultPtr syntax_analyzer_result = TreeRewriter(context).analyze(predicate, metadata_snapshot->getSampleBlock().getNamesAndTypesList());
            ExpressionAnalyzer analyzer(predicate, syntax_analyzer_result, context);
            /// FIXME: add_aliases is true to avoid adding source columns as outputs
            filter_dag.emplace(analyzer.getActionsDAG(/*add_aliases=*/ true));
            if (filter_dag->getOutputs().size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ActionsDAG contains more than 1 output for expression: {}", predicate->formatForLogging());
        }

        const auto & parts_ranges = RangesInDataParts{data_parts};

        const StorageSnapshotPtr storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);
        const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);

        std::optional<ReadFromMergeTree::Indexes> indexes;
        ReadFromMergeTree::buildIndexes(
            indexes,
            filter_dag ? &filter_dag.value() : nullptr,
            *merge_tree_data,
            parts_ranges,
            /*vector_search_parameters=*/ {},
            context,
            query_info,
            metadata_snapshot);

        ContextMutablePtr new_context = Context::createCopy(context);
        new_context->setSetting("use_skip_indexes_on_data_read", false);

        /// TODO: we may also want to support query condition cache here as well

        return MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
            parts_ranges,
            metadata_snapshot,
            snapshot_data.mutations_snapshot,
            new_context,
            indexes->key_condition,
            indexes->part_offset_condition,
            indexes->total_offset_condition,
            indexes->skip_indexes,
            reader_settings,
            getLogger("MergeTreeAnalyzeIndexSource"),
            num_streams,
            index_stats,
            indexes->use_skip_indexes,
            /* find_exact_ranges= */ false,
            /* is_final_query= */ false,
            /* is_parallel_reading_from_replicas= */ false);
    }

private:
    SharedHeader header;
    const StoragePtr storage;
    SelectQueryInfo query_info;
    size_t num_streams;
    ASTPtr predicate;
    MergeTreeData::DataPartsVector data_parts;
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
        std::shared_ptr<StorageMergeTreeAnalyzeIndexes> storage_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , num_streams(num_streams_)
        , log(&Poco::Logger::get("StorageMergeTreeAnalyzeIndexes"))
    {
    }

private:
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
        storage->source_table,
        getQueryInfo(),
        num_streams,
        storage->data_parts,
        storage->table_settings,
        storage->predicate,
        context)));
}


///
/// StorageMergeTreeAnalyzeIndex
///
StorageMergeTreeAnalyzeIndexes::StorageMergeTreeAnalyzeIndexes(
    const StorageID & table_id_,
    const StoragePtr & source_table_,
    const ColumnsDescription & columns,
    const String & parts_regexp_,
    const ASTPtr & predicate_)
    : IStorage(table_id_)
    , source_table(source_table_)
    , predicate(predicate_)
{
    const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree_data)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeAnalyzeIndexes expected MergeTree table, got: {}", source_table->getName());

    data_parts = merge_tree_data->getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });
    if (!parts_regexp_.empty())
    {
        OptimizedRegularExpression regexp(parts_regexp_);
        std::erase_if(data_parts, [&](const MergeTreeData::DataPartPtr & part) { return !regexp.match(part->name); });
    }

    table_settings = merge_tree_data->getSettings();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    setInMemoryMetadata(storage_metadata);
}

void StorageMergeTreeAnalyzeIndexes::read(
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

    auto sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
    auto this_ptr = std::static_pointer_cast<StorageMergeTreeAnalyzeIndexes>(shared_from_this());

    auto reading = std::make_unique<ReadFromMergeTreeAnalyzeIndexes>(
        column_names,
        query_info,
        num_streams,
        storage_snapshot,
        std::move(context),
        std::move(sample_block),
        std::move(this_ptr));

    query_plan.addStep(std::move(reading));
}


}
