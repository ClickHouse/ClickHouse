#include <unordered_set>
#include <Core/Field.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTreeAnalyzeIndex.h>
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
        const StorageMetadataPtr & metadata_snapshot_,
        const SelectQueryInfo & query_info_,
        size_t num_streams_,
        MergeTreeData::DataPartsVector data_parts_,
        MergeTreeSettingsPtr table_settings_,
        const ASTPtr & primary_key_predicate_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , header(std::move(header_))
        , metadata_snapshot(metadata_snapshot_)
        , query_info(query_info_)
        , num_streams(num_streams_)
        , primary_key_predicate(primary_key_predicate_)
        , data_parts(std::move(data_parts_))
        , table_settings(std::move(table_settings_))
    {
    }

    String getName() const override { return "MergeTreeAnalyzeIndex"; }

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

        const auto & primary_key = metadata_snapshot->getPrimaryKey();

        std::optional<ActionsDAG> filter_dag;
        if (primary_key_predicate)
        {
            /// FIXME:
            /// - use analyzer
            /// - use primary_key->getSampleBlock().getNamesAndTypesList() over metadata_snapshot->getSampleBlock().getNamesAndTypesList()
            TreeRewriterResultPtr syntax_analyzer_result = TreeRewriter(context).analyze(primary_key_predicate, metadata_snapshot->getSampleBlock().getNamesAndTypesList());
            ExpressionAnalyzer analyzer(primary_key_predicate, syntax_analyzer_result, context);
            /// FIXME: add_aliases is true to avoid adding source columns as outputs
            filter_dag.emplace(analyzer.getActionsDAG(/*add_aliases=*/ true));
            if (filter_dag->getOutputs().size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ActionsDAG contains more than 1 output for expression: {}", primary_key_predicate->formatForLogging());
        }
        ActionsDAGWithInversionPushDown filter_dag_for_key_condition(filter_dag ? filter_dag->getOutputs().front() : nullptr, context);
        auto indexes = ReadFromMergeTree::Indexes(KeyCondition{filter_dag_for_key_condition, context, primary_key.column_names, primary_key.expression});

        return MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
            RangesInDataParts{data_parts},
            metadata_snapshot,
            /* mutations_snapshot= */ {}, /// Used only for skip indexes
            context,
            indexes.key_condition,
            indexes.part_offset_condition,
            indexes.total_offset_condition,
            indexes.skip_indexes,
            reader_settings,
            getLogger("MergeTreeAnalyzeIndexSource"),
            num_streams,
            index_stats,
            /* use_skip_indexes= */ false,
            /* find_exact_ranges= */ false,
            /* is_final_query= */ false,
            /* is_parallel_reading_from_replicas= */ false);
    }

private:
    SharedHeader header;
    StorageMetadataPtr metadata_snapshot;
    SelectQueryInfo query_info;
    size_t num_streams;
    ASTPtr primary_key_predicate;
    MergeTreeData::DataPartsVector data_parts;
    MergeTreeSettingsPtr table_settings;

    bool analyzed = false;
};

///
/// ReadFromMergeTreeAnalyzeIndex
///
class ReadFromMergeTreeAnalyzeIndex : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromMergeTreeAnalyzeIndex"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromMergeTreeAnalyzeIndex(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        size_t num_streams_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        std::shared_ptr<StorageMergeTreeAnalyzeIndex> storage_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , num_streams(num_streams_)
        , log(&Poco::Logger::get("StorageMergeTreeAnalyzeIndex"))
    {
    }

private:
    std::shared_ptr<StorageMergeTreeAnalyzeIndex> storage;
    const size_t num_streams;
    Poco::Logger * log;
};

void ReadFromMergeTreeAnalyzeIndex::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    LOG_DEBUG(log, "Analyzing index from {} parts of table {}",
        storage->data_parts.size(),
        storage->source_table->getStorageID().getNameForLogs());

    pipeline.init(Pipe(std::make_shared<MergeTreeAnalyzeIndexSource>(
        getOutputHeader(),
        storage->source_table->getInMemoryMetadataPtr(),
        getQueryInfo(),
        num_streams,
        storage->data_parts,
        storage->table_settings,
        storage->primary_key_predicate,
        context)));
}


///
/// StorageMergeTreeAnalyzeIndex
///
StorageMergeTreeAnalyzeIndex::StorageMergeTreeAnalyzeIndex(
    const StorageID & table_id_,
    const StoragePtr & source_table_,
    const ColumnsDescription & columns,
    const String & parts_regexp_,
    const ASTPtr & primary_key_predicate_)
    : IStorage(table_id_)
    , source_table(source_table_)
    , primary_key_predicate(primary_key_predicate_)
{
    const auto * merge_tree_data = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree_data)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeAnalyzeIndex expected MergeTree table, got: {}", source_table->getName());

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

void StorageMergeTreeAnalyzeIndex::read(
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
    auto this_ptr = std::static_pointer_cast<StorageMergeTreeAnalyzeIndex>(shared_from_this());

    auto reading = std::make_unique<ReadFromMergeTreeAnalyzeIndex>(
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
