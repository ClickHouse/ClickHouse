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
#include <Interpreters/JoinedTables.h>
#include <Storages/StorageAggregatingMemory.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageValues.h>

#include <IO/WriteHelpers.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Sources/SourceWithProgress.h>
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

/// AggregatingOutputStream is used to feed data into Aggregator.
class AggregatingOutputStream : public IBlockOutputStream
{
public:
    AggregatingOutputStream(StorageAggregatingMemory & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_)
        : storage(storage_), metadata_snapshot(metadata_snapshot_), context(context_)
    {
    }

    // OutputStream structure is same as source (before aggregation).
    Block getHeader() const override { return storage.src_sample_block; }

    void write(const Block & block) override
    {
        // writeForDebug(block);

        // TODO: metadata_snapshot->check
        // TODO: update storage.total_size_bytes

        Block block_for_aggregation(block);

        AggregatedDataVariants & variants(*(storage.many_data)->variants[0]);
        ColumnRawPtrs key_columns(storage.aggregator_transform->params.keys_size);
        Aggregator::AggregateColumns aggregate_columns(storage.aggregator_transform->params.aggregates_size);
        bool no_more_keys = false;

        auto expression = storage.analysis_result.before_aggregation;
        auto expression_actions = std::make_shared<ExpressionActions>(expression);
        expression_actions->execute(block_for_aggregation);

        storage.aggregator_transform->aggregator.executeOnBlock(
            block_for_aggregation, variants, key_columns, aggregate_columns, no_more_keys);
    }

    // Used to run aggregation the usual way (via InterpreterSelectQuery),
    // and only purpose is to aid development.
    // TODO: remove this.
    void writeForDebug(const Block & block)
    {
        BlockInputStreamPtr in;

        /// We need keep InterpreterSelectQuery, until the processing will be finished, since:
        ///
        /// - We copy Context inside InterpreterSelectQuery to support
        ///   modification of context (Settings) for subqueries
        /// - InterpreterSelectQuery lives shorter than query pipeline.
        ///   It's used just to build the query pipeline and no longer needed
        /// - ExpressionAnalyzer and then, Functions, that created in InterpreterSelectQuery,
        ///   **can** take a reference to Context from InterpreterSelectQuery
        ///   (the problem raises only when function uses context from the
        ///    execute*() method, like FunctionDictGet do)
        /// - These objects live inside query pipeline (DataStreams) and the reference become dangling.
        std::optional<InterpreterSelectQuery> select;

        if (metadata_snapshot->hasSelectQuery())
        {
            auto query = metadata_snapshot->getSelectQuery();

            /// We create a table with the same name as original table and the same alias columns,
            ///  but it will contain single block (that is INSERT-ed into main table).
            /// InterpreterSelectQuery will do processing of alias columns.

            // TODO: seems like storage -> src_storage (block source)
            auto block_storage
                = StorageValues::create(storage.getStorageID(), metadata_snapshot->getColumns(), block, storage.getVirtuals());

            ContextPtr local_context = context;
            local_context->addViewSource(block_storage);

            auto select_query = query.inner_query->as<ASTSelectQuery &>();

            select.emplace(query.inner_query, local_context, SelectQueryOptions());
            auto select_result = select->execute();

            in = std::make_shared<MaterializingBlockInputStream>(select_result.getInputStream());

            /// Squashing is needed here because the materialized view query can generate a lot of blocks
            /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
            /// and two-level aggregation is triggered).
            in = std::make_shared<SquashingBlockInputStream>(
                in, context->getSettingsRef().min_insert_block_size_rows, context->getSettingsRef().min_insert_block_size_bytes);
            in = std::make_shared<ConvertingBlockInputStream>(
                in, metadata_snapshot->getSampleBlock(), ConvertingBlockInputStream::MatchColumnsMode::Name);
        }
        else
            in = std::make_shared<OneBlockInputStream>(block);

        in->readPrefix();

        while (Block result_block = in->read())
        {
            Nested::validateArraySizes(result_block);
            new_blocks.emplace_back(result_block);
        }

        in->readSuffix();
    }

    void writeSuffix() override
    {
        size_t inserted_bytes = 0;
        size_t inserted_rows = 0;

        for (const auto & block : new_blocks)
        {
            inserted_bytes += block.allocatedBytes();
            inserted_rows += block.rows();
        }

        std::lock_guard lock(storage.mutex);

        auto new_data = std::make_unique<Blocks>(*(storage.data.get()));
        new_data->insert(new_data->end(), new_blocks.begin(), new_blocks.end());

        storage.data.set(std::move(new_data));
        storage.total_size_bytes.fetch_add(inserted_bytes, std::memory_order_relaxed);
        storage.total_size_rows.fetch_add(inserted_rows, std::memory_order_relaxed);
    }

private:
    Blocks new_blocks;

    StorageAggregatingMemory & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
};


StorageAggregatingMemory::StorageAggregatingMemory(
    const StorageID & table_id_,
    ConstraintsDescription constraints_,
    const ASTCreateQuery & query,
    ContextPtr context_)
    : IStorage(table_id_), data(std::make_unique<const Blocks>())
{
    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for AggregatingMemory", ErrorCodes::INCORRECT_QUERY);

    // TODO: check GROUP BY inside this func
    auto select = SelectQueryDescription::getSelectQueryFromASTForAggr(query.select->clone());
    ASTPtr select_ptr = select.inner_query;

    auto select_context = std::make_unique<ContextPtr>(context_);

    /// Get info about source table.
    JoinedTables joined_tables(context_, select_ptr->as<ASTSelectQuery &>());
    StoragePtr source_storage = joined_tables.getLeftTableStorage();
    auto source_columns = source_storage->getInMemoryMetadata().getColumns().getAll();

    ColumnsDescription columns_before_aggr;
    for (const auto & column : source_columns)
    {
        ColumnDescription column_description(column.name, column.type);
        columns_before_aggr.add(column_description);
    }

    /// Get list of columns we get from select query.
    auto header = InterpreterSelectQuery(select_ptr, *select_context, SelectQueryOptions().analyze()).getSampleBlock();

    ColumnsDescription columns_after_aggr;

    /// Insert only columns returned by select.
    for (const auto & column : header)
    {
        ColumnDescription column_description(column.name, column.type);
        columns_after_aggr.add(column_description);
    }

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_after_aggr));
    storage_metadata.setConstraints(std::move(constraints_));
    storage_metadata.setSelectQuery(std::move(select));
    setInMemoryMetadata(storage_metadata);

    StorageInMemoryMetadata src_metadata;
    src_metadata.setColumns(std::move(columns_before_aggr));
    src_sample_block = src_metadata.getSampleBlock();

    auto src_metadata_snapshot = std::make_shared<StorageInMemoryMetadata>(src_metadata);

    Names required_result_column_names; // TODO:

    auto syntax_analyzer_result
        = TreeRewriter(*select_context)
              .analyzeSelect(
                  select_ptr, TreeRewriterResult(src_sample_block.getNamesAndTypesList()), {}, {}, required_result_column_names, {});

    auto query_analyzer = std::make_unique<SelectQueryExpressionAnalyzer>(
        select_ptr,
        syntax_analyzer_result,
        *select_context,
        src_metadata_snapshot,
        NameSet(required_result_column_names.begin(), required_result_column_names.end()));

    const Settings & settings = (*select_context)->getSettingsRef();

    analysis_result = ExpressionAnalysisResult(*query_analyzer, src_metadata_snapshot, false, false, false, nullptr, src_sample_block);

    Block header_before_aggregation = src_sample_block;
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
                              (*select_context)->getTemporaryVolume(),
                              settings.max_threads,
                              settings.min_free_disk_space_for_temporary_data,
                              true);

    aggregator_transform = std::make_shared<AggregatingTransformParams>(params, true);

    many_data = std::make_shared<ManyAggregatedData>(1);
}


Pipe StorageAggregatingMemory::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    // TODO: allow parallel read? (num_streams)
    // TODO: check if read by aggregation key is O(1)

    auto prepared_data = aggregator_transform->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));

    auto processor
        = std::make_shared<ConvertingAggregatedToChunksTransform>(aggregator_transform, std::move(prepared_data_ptr), num_streams);

    Pipes pipes;
    pipes.emplace_back(processor);

    auto final_projection = analysis_result.final_projection;
    auto expression_actions = std::make_shared<ExpressionActions>(final_projection);

    auto pipe = Pipe::unitePipes(std::move(pipes));
    pipe.addSimpleTransform([expression_actions](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression_actions);
    });

    return pipe;
}


BlockOutputStreamPtr StorageAggregatingMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    auto out = std::make_shared<AggregatingOutputStream>(*this, metadata_snapshot, context);
    return out;
}


void StorageAggregatingMemory::drop()
{
    data.set(std::make_unique<Blocks>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);

    // TODO: drop aggregator state?
}

static inline void updateBlockData(Block & old_block, const Block & new_block)
{
    for (const auto & it : new_block)
    {
        auto col_name = it.name;
        auto & col_with_type_name = old_block.getByName(col_name);
        col_with_type_name.column = it.column;
    }
}

void StorageAggregatingMemory::mutate(const MutationCommands & commands, ContextPtr context)
{
    // TODO: mutate is not supported?

    std::lock_guard lock(mutex);
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context);
    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, context, true);
    auto in = interpreter->execute();

    in->readPrefix();
    Blocks out;
    Block block;
    while ((block = in->read()))
    {
        out.push_back(block);
    }
    in->readSuffix();

    std::unique_ptr<Blocks> new_data;

    // all column affected
    if (interpreter->isAffectingAllColumns())
    {
        new_data = std::make_unique<Blocks>(out);
    }
    else
    {
        /// just some of the column affected, we need update it with new column
        new_data = std::make_unique<Blocks>(*(data.get()));
        auto data_it = new_data->begin();
        auto out_it = out.begin();

        while (data_it != new_data->end())
        {
            /// Mutation does not change the number of blocks
            assert(out_it != out.end());

            updateBlockData(*data_it, *out_it);
            ++data_it;
            ++out_it;
        }

        assert(out_it == out.end());
    }

    size_t rows = 0;
    size_t bytes = 0;
    for (const auto & buffer : *new_data)
    {
        rows += buffer.rows();
        bytes += buffer.bytes();
    }
    total_size_bytes.store(rows, std::memory_order_relaxed);
    total_size_rows.store(bytes, std::memory_order_relaxed);
    data.set(std::move(new_data));
}


void StorageAggregatingMemory::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    data.set(std::make_unique<Blocks>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);

    // TODO: clear aggregator state?
}

std::optional<UInt64> StorageAggregatingMemory::totalRows(const Settings &) const
{
    /// All modifications of these counters are done under mutex which automatically guarantees synchronization/consistency
    /// When run concurrently we are fine with any value: "before" or "after"
    return total_size_rows.load(std::memory_order_relaxed);

    // TODO: get info from aggregator?
}

std::optional<UInt64> StorageAggregatingMemory::totalBytes(const Settings &) const
{
    return total_size_bytes.load(std::memory_order_relaxed);

    // TODO: get info from aggregator?
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
        .supports_parallel_insert = true, // TODO: not sure
    });
}

}
