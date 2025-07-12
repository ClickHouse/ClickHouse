#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}


ReadFromObjectStorageStep::ReadFromObjectStorageStep(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const String & name_,
    const Names & columns_to_read,
    const NamesAndTypesList & virtual_columns_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const std::optional<DB::FormatSettings> & format_settings_,
    bool distributed_processing_,
    ReadFromFormatInfo info_,
    bool need_only_count_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_)
    : SourceStepWithFilter(info_.source_header, columns_to_read, query_info_, storage_snapshot_, context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , info(std::move(info_))
    , virtual_columns(virtual_columns_)
    , format_settings(format_settings_)
    , name(name_ + "ReadStep")
    , need_only_count(need_only_count_)
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , distributed_processing(distributed_processing_)
{
}

void ReadFromObjectStorageStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
    createIterator();
}

void ReadFromObjectStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    createIterator();

    Pipes pipes;
    auto context = getContext();
    size_t estimated_keys_count = iterator_wrapper->estimatedKeysCount();

    if (estimated_keys_count > 1)
        num_streams = std::min(num_streams, estimated_keys_count);
    else
    {
        /// The amount of keys (zero) was probably underestimated.
        /// We will keep one stream for this particular case.
        num_streams = 1;
    }

    auto parser_group = std::make_shared<FormatParserGroup>(context->getSettingsRef(), num_streams, filter_actions_dag, context);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<StorageObjectStorageSource>(
            getName(), object_storage, configuration, info, format_settings,
            context, max_block_size, iterator_wrapper, parser_group, need_only_count);

        pipes.emplace_back(std::move(source));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromObjectStorageStep::createIterator()
{
    if (iterator_wrapper)
        return;

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    auto context = getContext();
    iterator_wrapper = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(context), object_storage, distributed_processing,
        context, predicate, filter_actions_dag.get(), virtual_columns, nullptr, context->getFileProgressCallback());
}

}
