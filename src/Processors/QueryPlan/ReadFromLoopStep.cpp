#include <Interpreters/Context.h>
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

namespace DB
{
    namespace ErrorCodes
    {
        extern const int TOO_MANY_RETRIES_TO_FETCH_PARTS;
    }
    class PullingPipelineExecutor;

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
                : ISource(storage_snapshot_->getSampleBlockForColumns(column_names_))
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

        Chunk generate() override
        {
            while (true)
            {
                if (!loop)
                {
                    QueryPlan plan;
                    auto storage_snapshot_ = inner_storage->getStorageSnapshotForQuery(inner_storage->getInMemoryMetadataPtr(), nullptr, context);
                    inner_storage->read(
                            plan,
                            column_names,
                            storage_snapshot_,
                            query_info,
                            context,
                            processed_stage,
                            max_block_size,
                            num_streams);
                    if (plan.isInitialized())
                    {
                        auto builder = plan.buildQueryPipeline(
                                QueryPlanOptimizationSettings::fromContext(context),
                                BuildQueryPipelineSettings::fromContext(context));
                        QueryPlanResourceHolder resources;
                        auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), resources);
                        query_pipeline = QueryPipeline(std::move(pipe));
                        executor = std::make_unique<PullingPipelineExecutor>(query_pipeline);
                    }
                    loop = true;
                }
                Chunk chunk;
                if (executor && executor->pull(chunk))
                {
                    if (chunk)
                    {
                        retries_count = 0;
                        return chunk;
                    }

                }
                else
                {
                    ++retries_count;
                    if (retries_count > max_retries_count)
                        throw Exception(ErrorCodes::TOO_MANY_RETRIES_TO_FETCH_PARTS, "Too many retries to pull from storage");
                    loop = false;
                    executor.reset();
                    query_pipeline.reset();
                }
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
        // add retries. If inner_storage failed to pull X times in a row we'd better to fail here not to hang
        size_t retries_count = 0;
        size_t max_retries_count = 3;
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
            storage_snapshot_->getSampleBlockForColumns(column_names_),
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
            assert(output_header != std::nullopt);
            pipe = Pipe(std::make_shared<NullSource>(*output_header));
        }

        pipeline.init(std::move(pipe));
    }

}
