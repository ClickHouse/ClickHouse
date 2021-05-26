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
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/AggregatingTransform.cpp>
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

        Data(Aggregator::Params params_)
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
          aggregate_columns(storage.aggregator_transform->params.aggregates_size)
    {
        expression_actions = std::make_shared<ExpressionActions>(storage.analysis_result.before_aggregation);
    }

    // OutputStream structure is same as source (before aggregation).
    Block getHeader() const override { return storage.src_block_header; }

    void write(const Block & block) override
    {
        storage.src_metadata_snapshot->check(block, true);

        StoragePtr source_storage = storage.source_storage;
        auto query = metadata_snapshot->getSelectQuery();

        StoragePtr block_storage
            = StorageValues::create(source_storage->getStorageID(), source_storage->getInMemoryMetadataPtr()->getColumns(), block, source_storage->getVirtuals());

        ContextPtr local_context = Context::createCopy(context);
        local_context->addViewSource(block_storage);

        InterpreterSelectQuery select(query.inner_query, local_context, SelectQueryOptions(QueryProcessingStage::WithMergeableState));
        auto select_result = select.execute();

        BlockInputStreamPtr in;
        in = std::make_shared<MaterializingBlockInputStream>(select_result.getInputStream());
        in = std::make_shared<SquashingBlockInputStream>(
            in, context->getSettingsRef().min_insert_block_size_rows, context->getSettingsRef().min_insert_block_size_bytes);

        in->readPrefix();

        bool no_more_keys = false;
        while (Block result_block = in->read())
        {
            storage.aggregator_transform->aggregator.mergeBlock(result_block, variants, no_more_keys);
        }

        in->readSuffix();
    }

private:
    StorageAggregatingMemory & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;

    AggregatedDataVariants & variants;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    ExpressionActionsPtr expression_actions;
};

class StorageSource final : public ext::shared_ptr_helper<StorageSource>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSource>;
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
    constructor_context = context_;
    constructor_constraints = constraints_;
}

void StorageAggregatingMemory::lazy_initialize()
{
    if (is_initialized)
        return;

    std::lock_guard lock(mutex);

    if (is_initialized)
        return;

    ASTPtr select_ptr = select_query.inner_query;

    auto select_context = constructor_context;

    /// Get info about source table.
    JoinedTables joined_tables(constructor_context, select_ptr->as<ASTSelectQuery &>());
    source_storage = joined_tables.getLeftTableStorage();
    NamesAndTypesList source_columns = source_storage->getInMemoryMetadata().getColumns().getAll();

    ColumnsDescription columns_before_aggr;
    for (const auto & column : source_columns)
    {
        ColumnDescription column_description(column.name, column.type);
        columns_before_aggr.add(column_description);
    }

    /// Get list of columns we get from select query.
    Block header = InterpreterSelectQuery(select_ptr, select_context, SelectQueryOptions().analyze()).getSampleBlock();

    ColumnsDescription columns_after_aggr;

    /// Insert only columns returned by select.
    for (const auto & column : header)
    {
        ColumnDescription column_description(column.name, column.type);
        columns_after_aggr.add(column_description);
    }

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_after_aggr));
    storage_metadata.setConstraints(constructor_constraints);
    storage_metadata.setSelectQuery(select_query);
    setInMemoryMetadata(storage_metadata);

    StorageInMemoryMetadata src_metadata;
    src_metadata.setColumns(std::move(columns_before_aggr));
    src_block_header = src_metadata.getSampleBlock();

    src_metadata_snapshot = std::make_shared<StorageInMemoryMetadata>(src_metadata);

    Names required_result_column_names;

    auto syntax_analyzer_result
        = TreeRewriter(select_context)
              .analyzeSelect(
                  select_ptr, TreeRewriterResult(src_block_header.getNamesAndTypesList()), {}, {}, required_result_column_names, {});

    query_analyzer = std::make_unique<SelectQueryExpressionAnalyzer>(
        select_ptr,
        syntax_analyzer_result,
        select_context,
        src_metadata_snapshot,
        NameSet(required_result_column_names.begin(), required_result_column_names.end()));

    const Settings & settings = select_context->getSettingsRef();

    analysis_result = ExpressionAnalysisResult(*query_analyzer, src_metadata_snapshot, false, false, false, nullptr, src_block_header);

    Block header_before_aggregation = src_block_header;
    auto expression = analysis_result.before_aggregation;
    auto expression_actions = std::make_shared<ExpressionActions>(expression);
    expression_actions->execute(header_before_aggregation);

    ColumnNumbers keys;
    for (const auto & key : query_analyzer->aggregationKeys())
        keys.push_back(header_before_aggregation.getPositionByName(key.name));

    AggregateDescriptions aggregates = query_analyzer->aggregates();
    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(header_before_aggregation.getPositionByName(name));

    Aggregator::Params params(header_before_aggregation, keys, aggregates,
                              false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                              settings.group_by_two_level_threshold,
                              settings.group_by_two_level_threshold_bytes,
                              settings.max_bytes_before_external_group_by,
                              settings.empty_result_for_aggregation_by_empty_set,
                              select_context->getTemporaryVolume(),
                              settings.max_threads,
                              settings.min_free_disk_space_for_temporary_data,
                              true);

    aggregator_transform = std::make_shared<AggregatingTransformParams>(params, false);
    many_data = std::make_shared<ManyAggregatedData>(1);

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
    {
        AggregatingOutputStream os(*this, getInMemoryMetadataPtr(), constructor_context);
        os.write(src_block_header);
    }

    is_initialized = true;
}

void StorageAggregatingMemory::startup()
{
    try
    {
        lazy_initialize();
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
    lazy_initialize();
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    // TODO implement O(1) read by aggregation key

    auto prepared_data = aggregator_transform->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));

    ProcessorPtr source;

    auto filter_key = getFilterKeys(aggregator_transform->params, query_info);
    source = std::make_shared<ConvertingAggregatedToChunksTransform>(aggregator_transform, std::move(prepared_data_ptr), num_streams);

    StoragePtr mergable_storage = StorageSource::create(source_storage->getStorageID(), source_storage->getInMemoryMetadataPtr()->getColumns(), source);

    ContextPtr local_context = Context::createCopy(context);
    local_context->addViewSource(mergable_storage);
    
    InterpreterSelectQuery select(metadata_snapshot->getSelectQuery().inner_query, local_context, SelectQueryOptions());
    BlockIO select_result = select.execute();

    return QueryPipeline::getPipe(std::move(select_result.pipeline));
}

BlockOutputStreamPtr StorageAggregatingMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    lazy_initialize();
    auto out = std::make_shared<AggregatingOutputStream>(*this, metadata_snapshot, context);
    return out;
}

void StorageAggregatingMemory::drop()
{
    // TODO drop aggregator state
}

void StorageAggregatingMemory::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    // TODO clear aggregator state
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
