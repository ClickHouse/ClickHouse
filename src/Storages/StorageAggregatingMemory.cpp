#include <Common/Exception.h>

#include <DataTypes/NestedUtils.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/InterpreterCreateQuery.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>

#include <Storages/StorageAggregatingMemory.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageValues.h>

#include <IO/WriteHelpers.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingChunksTransform.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

class AggregationKeyMatcher
{
public:
    using Visitor = InDepthNodeVisitor<AggregationKeyMatcher, true>;

    struct Data
    {
        Aggregator::Params params;
        MutableColumns raw_key_columns;
        Block header;

        bool failed;

        explicit Data(Aggregator::Params params_)
            : params(params_),
              raw_key_columns(params.keys_size),
              header(params.getHeader(true)),
              failed(false)
        {
            for (size_t i = 0; i < params.keys_size; ++i)
            {
                raw_key_columns[i] = header.safeGetByPosition(i).type->createColumn();
                raw_key_columns[i]->reserve(1);
            }
        }

        bool hasAllKeys()
        {
            for (const auto & column : raw_key_columns)
                if (column->empty())
                    return false;

            return true;
        }
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !(node->as<ASTFunction>());
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        auto * function = ast->as<ASTFunction>();
        if (!function)
        {
            return;
        }

        if (function->name == "and")
        {
            /// Just need to visit children.
            Visitor(data).visit(function->arguments);
            return;
        }

        if (function->name != "equals")
        {
            /// Only simple cases with equals are supported.
            data.failed = true;
            return;
        }

        const auto & args = function->arguments->as<ASTExpressionList &>();
        const ASTIdentifier * ident;
        const IAST * value;

        if (args.children.size() != 2)
        {
            data.failed = true;
            return;
        }

        if ((ident = args.children.at(0)->as<ASTIdentifier>()))
            value = args.children.at(1).get();
        else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
            value = args.children.at(0).get();
        else
        {
            data.failed = true;
            return;
        }

        // TODO data.aggregation_keys can differ in names of aggregation keys (i.e. AS aliases)
        std::optional<size_t> key_position;
        for (size_t i = 0; i < data.params.keys_size; ++i)
        {
            if (data.header.getByPosition(i).name == ident->name())
            {
                key_position = i;
                break;
            }
        }

        /// This identifier is not a key.
        if (!key_position)
            return;

        /// function->name == "equals"
        if (const auto * literal = value->as<ASTLiteral>())
        {
            auto column_type = data.header.getByPosition(*key_position).type;

            auto converted_field = convertFieldToType(literal->value, *column_type);
            if (!converted_field.isNull() && data.raw_key_columns[*key_position]->empty())
            {
                data.raw_key_columns[*key_position]->insert(converted_field);
            }
        }
    }
};

using AggregationKeyVisitor = AggregationKeyMatcher::Visitor;

static std::optional<AggregationKeyVisitor::Data> getFilterKeys(const Aggregator::Params & params, const SelectQueryInfo & query_info)
{
    if (params.keys.empty())
        return {};

    const auto & select = query_info.query->as<ASTSelectQuery &>();
    ASTPtr where = select.where();
    if (!where)
        return {};

    AggregationKeyVisitor::Data data(params);
    AggregationKeyVisitor(data).visit(where);

    if (data.failed || !data.hasAllKeys())
        return {};

    return data;
}

/// AggregatingOutputStream is used to feed data into Aggregator.
class AggregatingMemorySink : public SinkToStorage
{
public:
    AggregatingMemorySink(StorageAggregatingMemory & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock()),
          storage(storage_),
          metadata_snapshot(metadata_snapshot_),
          context(context_),
          variants(*(storage.many_data)->variants[0])
    {
    }

    String getName() const override { return "AggregatingMemorySink"; }

    void consume(Chunk chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.detachColumns());
        /// TODO: fix this check.
        /// This block may have more columns then needed in case of MV.
        metadata_snapshot->check(block, true);

        auto query = storage.select_query.inner_query;

        StoragePtr block_storage
            = StorageValues::create(storage.getStorageID(), metadata_snapshot->getColumns(), block, storage.getVirtuals());

        InterpreterSelectQuery select(query, context, block_storage, nullptr, SelectQueryOptions(QueryProcessingStage::WithMergeableState));
        auto builder = select.buildQueryPipeline();

        builder = select.buildQueryPipeline();

        builder.addSimpleTransform([&](const Block & current_header)
        {
            return std::make_shared<MaterializingTransform>(current_header);
        });

        builder.addSimpleTransform([&](const Block & current_header)
        {
            return std::make_shared<SquashingChunksTransform>(
                current_header,
                context->getSettingsRef().min_insert_block_size_rows,
                context->getSettingsRef().min_insert_block_size_bytes);
        });

        auto header = builder.getHeader();
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        PullingPipelineExecutor executor(pipeline);

        Block result_block;
        bool no_more_keys = false;
        while (executor.pull(result_block))
        {
            std::unique_lock lock(storage.rwlock);
            storage.aggregator_transform_params->aggregator.mergeOnBlock(result_block, variants, no_more_keys);
        }
    }

private:
    StorageAggregatingMemory & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;

    AggregatedDataVariants & variants;
};

class StorageSource final : public shared_ptr_helper<StorageSource>, public IStorage
{
    friend struct shared_ptr_helper<StorageSource>;
public:
    String getName() const override { return "Source"; }

    Pipe read(
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override
    {
        return Pipe(source);
    }

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*to_stage*/,
        const StorageMetadataPtr &,
        SelectQueryInfo & /*info*/) const override
    {
        return QueryProcessingStage::WithMergeableState;
    }

private:
    ProcessorPtr source;
    std::shared_lock<std::shared_mutex> lock;

protected:
    StorageSource(const StorageID & table_id, const ColumnsDescription & columns_, ProcessorPtr source_, std::shared_lock<std::shared_mutex> lock_)
        : IStorage(table_id)
        , source(source_)
        , lock(std::move(lock_))
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_);
        setInMemoryMetadata(storage_metadata);
    }

    StorageSource(const StorageID & table_id, const ColumnsDescription & columns_) : IStorage(table_id)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_);
        setInMemoryMetadata(storage_metadata);
    }
};

static ASTSelectQuery * getFirstSelect(const IAST & select)
{
    const auto * new_select = select.as<ASTSelectWithUnionQuery>();
    if (!new_select || new_select->list_of_selects->children.empty())
        return nullptr;
    auto & new_inner_query = new_select->list_of_selects->children.at(0);
    if (auto * simple_select = new_inner_query->as<ASTSelectQuery>())
        return simple_select;
    else
        return getFirstSelect(*new_inner_query);
}

static ColumnsDescription blockToColumnsDescription(Block header)
{
    ColumnsDescription columns;
    for (const auto & column : header)
    {
        ColumnDescription column_description(column.name, column.type);
        columns.add(column_description);
    }

    return columns;
}

StorageAggregatingMemory::StorageAggregatingMemory(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    ConstraintsDescription constraints_,
    const ASTCreateQuery & query,
    ContextPtr context_)
    : IStorage(table_id_)
    , log(&Poco::Logger::get("StorageAggregatingMemory"))
{
    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for AggregatingMemory", ErrorCodes::INCORRECT_QUERY);

    // TODO check validity of aggregation query inside this func
    select_query = SelectQueryDescription::getSelectQueryFromASTForAggregation(query.select->clone());

    auto query_context = Context::createCopy(context_);
    query_context->makeQueryContext();

    ASTPtr select_ptr = select_query.inner_query;
    ASTSelectQuery * select = select_ptr->as<ASTSelectQuery>();
    if (!select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Invalid create query for AggregatingMemory. SELECT query is expected");

    bool has_specified_structure =
        query.columns_list && query.columns_list->columns && !query.columns_list->columns->children.empty();

    ColumnsDescription source_columns = columns_;

    if (!query.attach && !has_specified_structure)
    {
        /// Get info about source table.
        JoinedTables joined_tables(query_context, *select);
        auto source_storage = joined_tables.getLeftTableStorage();

        if (!source_storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Invalid create query for AggregatingMemory. Cannot find storage. Query {}",
                            queryToString(*query.select));

        source_columns = source_storage->getInMemoryMetadata().getColumns();

        /// Here we specify explicit columns list to ATTACH query (to be independent from storage).
        ASTPtr new_columns = InterpreterCreateQuery::formatColumns(source_columns);
        query.columns_list->setOrReplace(query.columns_list->columns, new_columns);
    }

    /// We always remove table expression from select. Because it is used only to get table structure.
    /// But next time after ATTACH it is explicitly specified.
    getFirstSelect(*query.select)->setExpression(ASTSelectQuery::Expression::TABLES, nullptr);

    /// Internal select reads from itself. It is ok, cause we will use view source.
    select->replaceDatabaseAndTable(getStorageID());

    query_context->addViewSource(
        StorageValues::create(getStorageID(), source_columns, Block(), getVirtuals()));

    /// Get list of columns we get from select query.
    Block header = InterpreterSelectQuery(select_ptr, query_context, SelectQueryOptions().analyze()).getSampleBlock();

    /// Init metadata for reads from this storage.
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(blockToColumnsDescription(header));
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setSelectQuery(select_query);
    setInMemoryMetadata(storage_metadata);

    /// Init metadata for writes.
    StorageInMemoryMetadata src_metadata;
    src_metadata.setColumns(source_columns);
    src_metadata_snapshot = std::make_shared<StorageInMemoryMetadata>(src_metadata);

    /// Create AggregatingStep to extract params from it.
    InterpreterSelectQuery select_interpreter(select_ptr, query_context, SelectQueryOptions(QueryProcessingStage::WithMergeableState).analyze());

    if (!select_interpreter.getAnalysisResult().need_aggregate)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Query for AggregatingMemory storage must have aggregation. Query: {}",
                        queryToString(select_ptr));

    QueryPlan query_plan;
    select_interpreter.buildQueryPlan(query_plan);

    const auto * aggregating_step = typeid_cast<const AggregatingStep *>(query_plan.getLastStep()->step.get());
    if (!aggregating_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregatingStep is not found for query. Last step is {}",
                        query_plan.getLastStep()->step->getName());

    Aggregator::Params aggr_params = aggregating_step->getParams();

    const Settings & settings = query_context->getSettingsRef();
    Aggregator::Params params(aggr_params.src_header, aggr_params.keys, aggr_params.aggregates,
                              false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                              settings.group_by_two_level_threshold,
                              settings.group_by_two_level_threshold_bytes,
                              settings.max_bytes_before_external_group_by,
                              settings.empty_result_for_aggregation_by_empty_set,
                              query_context->getTemporaryVolume(),
                              settings.max_threads,
                              settings.min_free_disk_space_for_temporary_data,
                              settings.compile_aggregate_expressions,
                              settings.min_count_to_compile_aggregate_expression,
                              true);

    aggregator_transform_params = std::make_shared<AggregatingTransformParams>(params, false);
    initState(query_context);
}

void StorageAggregatingMemory::initState(ContextPtr context)
{
    many_data = std::make_shared<ManyAggregatedData>(1);

    /// If there was no data, and we aggregate without keys,
    /// and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (aggregator_transform_params->params.keys_size == 0 && !aggregator_transform_params->params.empty_result_for_aggregation_by_empty_set)
    {
        auto sink = std::make_shared<AggregatingMemorySink>(*this, src_metadata_snapshot, context);
        QueryPipeline pipeline(std::move(std::move(sink)));
        PushingPipelineExecutor executor(pipeline);
        executor.push(src_metadata_snapshot->getSampleBlock());
    }
}

void StorageAggregatingMemory::startup()
{
}

Pipe StorageAggregatingMemory::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    // TODO implement O(1) read by aggregation key
    auto filter_key = getFilterKeys(aggregator_transform_params->params, query_info);

    std::shared_lock lock(rwlock);

    auto prepared_data = aggregator_transform_params->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));

    ProcessorPtr source;

    if (filter_key.has_value())
    {
        ColumnRawPtrs key_columns(filter_key->raw_key_columns.size());
        for (size_t i = 0; i < key_columns.size(); ++i)
            key_columns[i] = filter_key->raw_key_columns[i].get();

        Block block = aggregator_transform_params->aggregator.readBlockByFilterKeys(prepared_data_ptr, key_columns);

        Chunk chunk(block.getColumns(), block.rows());
        source = std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), std::move(chunk));
    }
    else
    {
        source = std::make_shared<ConvertingAggregatedToChunksTransform>(aggregator_transform_params, std::move(prepared_data_ptr), num_streams);
    }

    StoragePtr mergable_storage = StorageSource::create(getStorageID(), src_metadata_snapshot->getColumns(), source, std::move(lock));

    InterpreterSelectQuery select(metadata_snapshot->getSelectQuery().inner_query, context, mergable_storage);
    return QueryPipelineBuilder::getPipe(select.buildQueryPipeline());
}

SinkToStoragePtr StorageAggregatingMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    return std::make_shared<AggregatingMemorySink>(*this, metadata_snapshot, context);
}

void StorageAggregatingMemory::drop()
{
    /// Drop aggregation state.
    many_data = nullptr;
}

void StorageAggregatingMemory::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, TableExclusiveLockHolder &)
{
    /// Assign fresh state.
    initState(context);
}

std::optional<UInt64> StorageAggregatingMemory::totalRows(const Settings &) const
{
    if (!many_data)
        return 0;

    return many_data->variants[0]->size();
}

std::optional<UInt64> StorageAggregatingMemory::totalBytes(const Settings &) const
{
    // Not possible to determine precisely.
    // TODO: can implement estimation from hash table size and size of arenas.
    return {};
}

void registerStorageAggregatingMemory(StorageFactory & factory)
{
    factory.registerStorage("AggregatingMemory", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageAggregatingMemory::create(args.table_id, args.columns, args.constraints, args.query, args.getLocalContext());
    },
    {
        .supports_parallel_insert = true, // TODO not sure
    });
}

}
