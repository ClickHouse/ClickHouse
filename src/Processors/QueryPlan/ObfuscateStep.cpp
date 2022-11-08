#include <Processors/QueryPlan/ObfuscateStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/ObfuscateTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/Obfuscator/Obfuscator.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ObfuscateStep::ObfuscateStep(
    const DataStream & input_stream_,
    TemporaryDataOnDiskScopePtr tmp_data_scope_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , tmp_data_scope(std::move(tmp_data_scope_))
{
    assert(tmp_data_scope);
}

void ObfuscateStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    MarkovModelParameters markov_model_params;

    markov_model_params.order = 5;
    markov_model_params.frequency_cutoff = 5;
    markov_model_params.num_buckets_cutoff = 0;
    markov_model_params.frequency_add = 0;
    markov_model_params.frequency_desaturate = 0;
    markov_model_params.determinator_sliding_window_size = 8;

    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ObfuscateTransform>(
            header,
            std::make_unique<TemporaryDataOnDisk>(tmp_data_scope),
            markov_model_params,
            /*seed*/ 0,
            /*keep_original_data_*/ false
        );
    });
}

void ObfuscateStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Obfuscate " << '\n';
}

void ObfuscateStep::describeActions(JSONBuilder::JSONMap & /*map*/) const
{
}

void ObfuscateStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
}


}
