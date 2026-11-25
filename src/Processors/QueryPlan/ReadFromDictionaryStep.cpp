#include <Processors/QueryPlan/ReadFromDictionaryStep.h>

#include <Core/Settings.h>
#include <Dictionaries/IDictionary.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 query_plan_max_step_description_length;
}

ReadFromDictionaryStep::ReadFromDictionaryStep(
    SharedHeader output_header_,
    StoragePtr storage_,
    std::shared_ptr<const IDictionary> dictionary_,
    Names column_names_,
    size_t max_block_size_,
    size_t num_streams_,
    std::shared_ptr<const StorageLimitsList> storage_limits_,
    ContextPtr context_)
    : ISourceStep(std::move(output_header_))
    , storage(std::move(storage_))
    , dictionary(std::move(dictionary_))
    , column_names(std::move(column_names_))
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , storage_limits(std::move(storage_limits_))
    , context(std::move(context_))
{
    setStepDescription(
        storage->getStorageID().getFullNameNotQuoted(), context->getSettingsRef()[Setting::query_plan_max_step_description_length]);
}

void ReadFromDictionaryStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = dictionary->read(column_names, max_block_size, num_streams);

    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(getOutputHeader()));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    /// Some layouts ignore `column_names` and return all the columns of the dictionary or of its source.
    if (!blocksHaveEqualStructure(pipe.getHeader(), *getOutputHeader()))
    {
        auto converting_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            getOutputHeader()->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);
        auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
        pipe.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<ExpressionTransform>(header, converting_actions);
        });
    }

    /// FIXME: the output is not resized to `num_streams`, because processing after reading from dictionaries
    /// in parallel can give a wrong `count()`, see test_dictionaries_redis/test_long.py::test_redis_dict_long

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}
