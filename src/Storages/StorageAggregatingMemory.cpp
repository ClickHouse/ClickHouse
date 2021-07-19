#include <cassert>
#include <Common/Exception.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>

#include <DataTypes/NestedUtils.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/StorageAggregatingMemory.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageValues.h>

#include <IO/WriteHelpers.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_QUERY;
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
        int key_position = -1;
        for (size_t i = 0; i < data.params.keys_size; ++i)
        {
            if (data.header.getByPosition(i).name == ident->name())
            {
                key_position = i;
                break;
            }
        }

        /// This identifier is not a key.
        if (key_position == -1)
            return;

        /// function->name == "equals"
        if (const auto * literal = value->as<ASTLiteral>())
        {
            auto column_type = data.header.getByPosition(key_position).type;

            auto converted_field = convertFieldToType(literal->value, *column_type);
            if (!converted_field.isNull() && data.raw_key_columns[key_position]->empty())
            {
                data.raw_key_columns[key_position]->insert(converted_field);
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
class AggregatingOutputStream : public IBlockOutputStream
{
public:
    AggregatingOutputStream(StorageAggregatingMemory & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_)
        : storage(storage_),
          metadata_snapshot(metadata_snapshot_),
          context(context_),
          variants(*(storage.many_data)->variants[0]),
          key_columns(storage.aggregator_transform->params.keys_size),
          aggregate_columns(storage.aggregator_transform->params.aggregates_size) {}

    // OutputStream structure is same as source (before aggregation).
    Block getHeader() const override { return storage.src_metadata_snapshot->getSampleBlock(); }

    void writePrefix() override
    {
    }

    void write(const Block & block) override
    {
        storage.src_metadata_snapshot->check(block, true);

        StoragePtr source_storage = storage.source_storage;
        auto query = metadata_snapshot->getSelectQuery();

        StoragePtr block_storage
            = StorageValues::create(source_storage->getStorageID(), source_storage->getInMemoryMetadataPtr()->getColumns(), block, source_storage->getVirtuals());

        InterpreterSelectQuery select(query.inner_query, context, block_storage, nullptr, SelectQueryOptions(QueryProcessingStage::WithMergeableState));
        auto select_result = select.execute();

        BlockInputStreamPtr in;
        in = std::make_shared<MaterializingBlockInputStream>(select_result.getInputStream());
        in = std::make_shared<SquashingBlockInputStream>(
            in, context->getSettingsRef().min_insert_block_size_rows, context->getSettingsRef().min_insert_block_size_bytes);

        in->readPrefix();

        bool no_more_keys = false;
        while (Block result_block = in->read())
        {
            std::unique_lock lock(storage.rwlock);
            storage.aggregator_transform->aggregator.mergeBlock(result_block, variants, no_more_keys);
        }

        in->readSuffix();
    }

    void writeSuffix() override
    {
    }

private:
    StorageAggregatingMemory & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;

    AggregatedDataVariants & variants;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
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

protected:
    StorageSource(const StorageID & table_id, const ColumnsDescription & columns_, ProcessorPtr source_)
        : IStorage(table_id),
          source(source_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_);
        setInMemoryMetadata(storage_metadata);
    }
};

StorageAggregatingMemory::StorageAggregatingMemory(
    const StorageID & table_id_,
    ConstraintsDescription constraints_,
    const ASTCreateQuery & query,
    ContextPtr context_)
    : IStorage(table_id_),
      log(&Poco::Logger::get("StorageAggregatingMemory"))
{
    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for AggregatingMemory", ErrorCodes::INCORRECT_QUERY);

    // TODO check validity of aggregation query inside this func
    select_query = SelectQueryDescription::getSelectQueryFromASTForAggr(query.select->clone());

    ContextMutablePtr context_copy = Context::createCopy(context_);
    context_copy->makeQueryContext();
    query_context = context_copy;

    constructor_constraints = constraints_;
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

static ColumnsDescription nameTypeListToColumnsDescription(NamesAndTypesList list)
{
    ColumnsDescription columns;
    for (const auto & column : list)
    {
        ColumnDescription column_description(column.name, column.type);
        columns.add(column_description);
    }

    return columns;
}

static const AggregatingStep * extractAggregatingStepFromPlan(const QueryPlan::Nodes & nodes)
{
    for (auto && node : nodes)
    {
        AggregatingStep * step_ptr = dynamic_cast<AggregatingStep *>(node.step.get());
        if (step_ptr)
            return step_ptr;
    }

    throw Exception("AggregatingStep is not found for query", ErrorCodes::INCORRECT_QUERY);
}

void StorageAggregatingMemory::lazyInit()
{
    if (is_initialized)
        return;

    std::lock_guard lock(mutex);

    if (is_initialized)
        return;

    ASTPtr select_ptr = select_query.inner_query;

    /// Get info about source table.
    JoinedTables joined_tables(query_context, select_ptr->as<ASTSelectQuery &>());
    source_storage = joined_tables.getLeftTableStorage();
    NamesAndTypesList source_columns = source_storage->getInMemoryMetadata().getColumns().getAll();

    /// Get list of columns we get from select query.
    Block header = InterpreterSelectQuery(select_ptr, query_context, SelectQueryOptions().analyze()).getSampleBlock();

    /// Init metadata for reads from this storage.
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(blockToColumnsDescription(header));
    storage_metadata.setConstraints(constructor_constraints);
    storage_metadata.setSelectQuery(select_query);
    setInMemoryMetadata(storage_metadata);

    /// Init metadata for writes.
    StorageInMemoryMetadata src_metadata;
    src_metadata.setColumns(nameTypeListToColumnsDescription(source_columns));
    src_metadata_snapshot = std::make_shared<StorageInMemoryMetadata>(src_metadata);

    /// Create AggregatingStep to extract params from it.
    InterpreterSelectQuery select_interpreter(select_ptr, query_context, SelectQueryOptions(QueryProcessingStage::WithMergeableState).analyze());
    QueryPlan query_plan;
    select_interpreter.buildQueryPlan(query_plan);

    const AggregatingStep * aggregating_step = extractAggregatingStepFromPlan(query_plan.nodes);
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

    aggregator_transform = std::make_shared<AggregatingTransformParams>(params, false);
    initState(query_context);
    is_initialized = true;
}

void StorageAggregatingMemory::initState(ContextPtr context)
{
    many_data = std::make_shared<ManyAggregatedData>(1);

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (aggregator_transform->params.keys_size == 0 && !aggregator_transform->params.empty_result_for_aggregation_by_empty_set)
    {
        AggregatingOutputStream os(*this, getInMemoryMetadataPtr(), context);
        os.write(src_metadata_snapshot->getSampleBlock());
    }
}

void StorageAggregatingMemory::startup()
{
    try
    {
        lazyInit();
    }
    catch (Exception & e)
    {
        e.addMessage("Failed to initialize AggregatingMemory on startup");
        LOG_ERROR(log, "{}", getCurrentExceptionMessage(true));
    }
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
    lazyInit();
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    // TODO implement O(1) read by aggregation key
    auto filter_key = getFilterKeys(aggregator_transform->params, query_info);

    std::shared_lock lock(rwlock);

    auto prepared_data = aggregator_transform->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));

    ProcessorPtr source;
    
    if (filter_key.has_value())
    {
        ColumnRawPtrs key_columns(filter_key->raw_key_columns.size());
        for (size_t i = 0; i < key_columns.size(); ++i)
            key_columns[i] = filter_key->raw_key_columns[i].get();

        Block block = aggregator_transform->aggregator.readBlockByFilterKeys(prepared_data_ptr, key_columns);

        Chunk chunk(block.getColumns(), block.rows());
        source = std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), std::move(chunk));
    } else {
        source = std::make_shared<ConvertingAggregatedToChunksTransform>(aggregator_transform, std::move(prepared_data_ptr), num_streams);
    }

    StoragePtr mergable_storage = StorageSource::create(source_storage->getStorageID(), source_storage->getInMemoryMetadataPtr()->getColumns(), source);

    InterpreterSelectQuery select(metadata_snapshot->getSelectQuery().inner_query, context, mergable_storage);
    BlockIO select_result = select.execute();

    return QueryPipeline::getPipe(std::move(select_result.pipeline));
}

BlockOutputStreamPtr StorageAggregatingMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    lazyInit();

    auto out = std::make_shared<AggregatingOutputStream>(*this, metadata_snapshot, context);
    return out;
}

void StorageAggregatingMemory::drop()
{
    if (!is_initialized)
        return;

    /// Drop aggregation state.
    many_data = nullptr;
}

void StorageAggregatingMemory::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, TableExclusiveLockHolder &)
{
    lazyInit();

    /// Assign fresh state.
    initState(context);
}

std::optional<UInt64> StorageAggregatingMemory::totalRows(const Settings &) const
{
    if (!is_initialized)
        return 0;

    return many_data->variants[0]->size();
}

std::optional<UInt64> StorageAggregatingMemory::totalBytes(const Settings &) const
{
    if (!is_initialized)
        return 0;

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

        return StorageAggregatingMemory::create(args.table_id, args.constraints, args.query, args.getLocalContext());
    },
    {
        .supports_parallel_insert = true, // TODO not sure
    });
}

}
