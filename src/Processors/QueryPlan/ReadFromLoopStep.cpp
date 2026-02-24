#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromLoopStep.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Storages/IStorage.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_RETRIES_TO_FETCH_PARTS;
}

class PullingPipelineExecutor;

namespace
{
    void buildInterpreterQueryPlan(
        QueryPlan & plan,
        const String & database,
        const String & table,
        const Names & column_names,
        const SelectQueryInfo & query_info,
        ContextPtr context)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_expr_list = make_intrusive<ASTExpressionList>();
        for (const auto & col_name : column_names)
            select_expr_list->children.push_back(make_intrusive<ASTIdentifier>(col_name));
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));

        select_query->replaceDatabaseAndTable(database, table);

        auto select_ast = make_intrusive<ASTSelectWithUnionQuery>();
        select_ast->list_of_selects = make_intrusive<ASTExpressionList>();
        select_ast->list_of_selects->children.push_back(select_query);
        select_ast->children.push_back(select_ast->list_of_selects);

        auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false);

        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter(select_ast, context, options, column_names);
            if (query_info.storage_limits)
                interpreter.addStorageLimits(*query_info.storage_limits);
            plan = std::move(interpreter).extractQueryPlan();
        }
        else
        {
            InterpreterSelectWithUnionQuery interpreter(select_ast, context, options, column_names);
            if (query_info.storage_limits)
                interpreter.addStorageLimits(*query_info.storage_limits);
            interpreter.buildQueryPlan(plan);
        }
    }
}

class LoopSource : public ISource
{
public:

    LoopSource(
            const Names & column_names_,
            const SelectQueryInfo & query_info_,
            const StorageSnapshotPtr & storage_snapshot_,
            ContextPtr & context_,
            QueryProcessingStage::Enum processed_stage_,
            StoragePtr inner_storage_,
            size_t max_block_size_,
            size_t num_streams_)
            : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)))
            , column_names(column_names_)
            , query_info(query_info_)
            , storage_snapshot(storage_snapshot_)
            , processed_stage(processed_stage_)
            , context(context_)
            , inner_storage(std::move(inner_storage_))
            , max_block_size(max_block_size_)
            , num_streams(num_streams_)
    {
    }

    String getName() const override { return "Loop"; }

    void initLoop()
    {
        if (loop)
            return;

        QueryPlan plan;

        if (DatabaseCatalog::instance().isTableExist(inner_storage->getStorageID(), context))
        {
            inner_context = Context::createCopy(context);
            const auto & storage_id = inner_storage->getStorageID();
            buildInterpreterQueryPlan(
                plan, storage_id.database_name, storage_id.table_name,
                column_names, query_info, inner_context);
        }
        else
        {
            auto storage_snapshot_ = inner_storage->getStorageSnapshot(inner_storage->getInMemoryMetadataPtr(), context);
            inner_storage->read(
                    plan,
                    column_names,
                    storage_snapshot_,
                    query_info,
                    context,
                    processed_stage,
                    max_block_size,
                    num_streams);
        }

        if (plan.isInitialized())
        {
            auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
            QueryPlanResourceHolder resources;
            auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), resources);
            query_pipeline = QueryPipeline(std::move(pipe));
            query_pipeline.addResources(std::move(resources));
            executor = std::make_unique<PullingPipelineExecutor>(query_pipeline);
        }
        loop = true;
    }

    Chunk generate() override
    {
        while (true)
        {
            if (!loop)
                initLoop();

            Chunk chunk;

            if (query_info.trivial_limit > 0 && rows_read >= query_info.trivial_limit)
                return chunk;

            if (executor && executor->pull(chunk))
            {
                rows_read += chunk.getNumRows();
                retries_count = 0;
                if (query_info.trivial_limit == 0 || rows_read <= query_info.trivial_limit)
                    return chunk;

                size_t remaining_rows = query_info.trivial_limit + chunk.getNumRows() - rows_read;
                auto columns = chunk.detachColumns();
                for (auto & col : columns)
                    col = col->cut(0, remaining_rows);

                return {std::move(columns), remaining_rows};
            }

            ++retries_count;
            if (retries_count > max_retries_count)
                throw Exception(ErrorCodes::TOO_MANY_RETRIES_TO_FETCH_PARTS, "Too many retries to pull from storage");

            loop = false;
            executor.reset();
            query_pipeline.reset();
        }
    }

private:
    const Names column_names;
    SelectQueryInfo query_info;
    const StorageSnapshotPtr storage_snapshot;
    QueryProcessingStage::Enum processed_stage;
    ContextPtr context;
    StoragePtr inner_storage;
    size_t max_block_size;
    size_t num_streams;
    ContextPtr inner_context;
    // add retries. If inner_storage failed to pull X times in a row we'd better to fail here not to hang
    size_t retries_count = 0;
    size_t max_retries_count = 3;
    size_t rows_read = 0;
    bool loop = false;
    QueryPipeline query_pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
};

static ContextPtr disableParallelReplicas(ContextPtr context)
{
    auto modified_context = Context::createCopy(context);
    modified_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
    return modified_context;
}

ReadFromLoopStep::ReadFromLoopStep(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        QueryProcessingStage::Enum processed_stage_,
        StoragePtr inner_storage_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(
        std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)),
        column_names_,
        query_info_,
        storage_snapshot_,
        disableParallelReplicas(context_))
        , column_names(column_names_)
        , processed_stage(processed_stage_)
        , inner_storage(std::move(inner_storage_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
{
}

Pipe ReadFromLoopStep::makePipe()
{
    return Pipe(std::make_shared<LoopSource>(
            column_names, query_info, storage_snapshot, context, processed_stage, inner_storage, max_block_size, num_streams));
}

void ReadFromLoopStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        chassert(output_header != nullptr);
        pipe = Pipe(std::make_shared<NullSource>(output_header));
    }

    pipeline.init(std::move(pipe));
}

}
