#pragma once
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

template <typename StorageSettings>
class ReadFromStorageObejctStorage : public SourceStepWithFilter
{
public:
    using Storage = StorageObjectStorage<StorageSettings>;
    using Source = StorageObjectStorageSource<StorageSettings>;

    ReadFromStorageObejctStorage(
        ObjectStoragePtr object_storage_,
        Storage::ConfigurationPtr configuration_,
        const String & name_,
        const NamesAndTypesList & virtual_columns_,
        const std::optional<DB::FormatSettings> & format_settings_,
        bool distributed_processing_,
        ReadFromFormatInfo info_,
        const bool need_only_count_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(DataStream{.header = info_.source_header})
        , object_storage(object_storage_)
        , configuration(configuration_)
        , context(std::move(context_))
        , info(std::move(info_))
        , virtual_columns(virtual_columns_)
        , format_settings(format_settings_)
        , name(name_ + "Source")
        , need_only_count(need_only_count_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
        , distributed_processing(distributed_processing_)
    {
    }

    std::string getName() const override { return name; }

    void applyFilters() override
    {
        auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
        const ActionsDAG::Node * predicate = nullptr;
        if (filter_actions_dag)
            predicate = filter_actions_dag->getOutputs().at(0);

        createIterator(predicate);
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        createIterator(nullptr);

        Pipes pipes;
        for (size_t i = 0; i < num_streams; ++i)
        {
            pipes.emplace_back(std::make_shared<Source>(
                getName(), object_storage, configuration, info, format_settings,
                context, max_block_size, iterator_wrapper, need_only_count));
        }

        auto pipe = Pipe::unitePipes(std::move(pipes));
        if (pipe.empty())
            pipe = Pipe(std::make_shared<NullSource>(info.source_header));

        for (const auto & processor : pipe.getProcessors())
            processors.emplace_back(processor);

        pipeline.init(std::move(pipe));
    }

private:
    ObjectStoragePtr object_storage;
    Storage::ConfigurationPtr configuration;
    ContextPtr context;

    const ReadFromFormatInfo info;
    const NamesAndTypesList virtual_columns;
    const std::optional<DB::FormatSettings> format_settings;
    const String name;
    const bool need_only_count;
    const size_t max_block_size;
    const size_t num_streams;
    const bool distributed_processing;

    std::shared_ptr<typename Source::IIterator> iterator_wrapper;

    void createIterator(const ActionsDAG::Node * predicate)
    {
        if (!iterator_wrapper)
        {
            iterator_wrapper = Source::createFileIterator(
                configuration, object_storage, distributed_processing, context,
                predicate, virtual_columns, nullptr, context->getFileProgressCallback());
        }
    }
};

}
