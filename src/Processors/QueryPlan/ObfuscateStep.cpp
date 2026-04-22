#include <Processors/QueryPlan/ObfuscateStep.h>

#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Processors/Sources/ObfuscateSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ObfuscateStep::ObfuscateStep(
    SharedHeader output_header_,
    ASTPtr inner_query_,
    Names column_names_,
    ContextPtr context_,
    UInt64 seed_)
    : ISourceStep(std::move(output_header_))
    , inner_query(std::move(inner_query_))
    , column_names(std::move(column_names_))
    , context(std::move(context_))
    , seed(seed_)
{
    markov_model_params.order = 5;
    markov_model_params.frequency_cutoff = 5;
    markov_model_params.num_buckets_cutoff = 0;
    markov_model_params.frequency_add = 0;
    markov_model_params.frequency_desaturate = 0;
    markov_model_params.determinator_sliding_window_size = 8;
}

void ObfuscateStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<ObfuscateSource>(
        getOutputHeader(),
        inner_query,
        column_names,
        context,
        markov_model_params,
        seed)));
}

void ObfuscateStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Obfuscate\n";
}

void ObfuscateStep::describeActions(JSONBuilder::JSONMap & /*map*/) const
{
}

}
