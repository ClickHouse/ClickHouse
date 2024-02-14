#include <Storages/ObjectStorage/ReadFromStorageObjectStorage.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ReadFromStorageObejctStorage::ReadFromStorageObejctStorage(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const String & name_,
    const NamesAndTypesList & virtual_columns_,
    const std::optional<DB::FormatSettings> & format_settings_,
    const StorageObjectStorageSettings & query_settings_,
    bool distributed_processing_,
    ReadFromFormatInfo info_,
    SchemaCache & schema_cache_,
    const bool need_only_count_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_,
    CurrentMetrics::Metric metric_threads_count_,
    CurrentMetrics::Metric metric_threads_active_,
    CurrentMetrics::Metric metric_threads_scheduled_)
    : SourceStepWithFilter(DataStream{.header = info_.source_header})
    , WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , info(std::move(info_))
    , virtual_columns(virtual_columns_)
    , format_settings(format_settings_)
    , query_settings(query_settings_)
    , schema_cache(schema_cache_)
    , name(name_ + "Source")
    , need_only_count(need_only_count_)
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , distributed_processing(distributed_processing_)
    , metric_threads_count(metric_threads_count_)
    , metric_threads_active(metric_threads_active_)
    , metric_threads_scheduled(metric_threads_scheduled_)
{
}

void ReadFromStorageObejctStorage::createIterator(const ActionsDAG::Node * predicate)
{
    if (!iterator_wrapper)
    {
        auto context = getContext();
        iterator_wrapper = StorageObjectStorageSource::createFileIterator(
            configuration, object_storage, distributed_processing, context, predicate,
            virtual_columns, nullptr, query_settings.list_object_keys_size, metric_threads_count,
            metric_threads_active, metric_threads_scheduled, context->getFileProgressCallback());
    }
}

void ReadFromStorageObejctStorage::applyFilters()
{
    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);
    createIterator(predicate);
}

void ReadFromStorageObejctStorage::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    createIterator(nullptr);
    auto context = getContext();

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto threadpool = std::make_shared<ThreadPool>(
            metric_threads_count, metric_threads_active, metric_threads_scheduled, /* max_threads */1);

        auto source = std::make_shared<StorageObjectStorageSource>(
            getName(), object_storage, configuration, info, format_settings, query_settings,
            context, max_block_size, iterator_wrapper, need_only_count, schema_cache,
            std::move(threadpool), metric_threads_count, metric_threads_active, metric_threads_scheduled);

        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}
