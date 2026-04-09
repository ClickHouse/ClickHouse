#include <Processors/QueryPlan/ObfuscateStep.h>
#include <Processors/Port.h>
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
    const SharedHeader & input_header_,
    TemporaryDataOnDiskScopePtr tmp_data_scope_)
    : ITransformingStep(input_header_, input_header_, getTraits())
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

    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<ObfuscateTransform>(
            *header,
            tmp_data_scope,
            markov_model_params,
            /*seed*/ 0,
            /*keep_original_data*/ false
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

}
