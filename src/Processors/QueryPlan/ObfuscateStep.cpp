#include <Processors/QueryPlan/ObfuscateStep.h>

#include <Core/Settings.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Sources/ObfuscateSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Common/SipHash.h>
#include <Common/randomSeed.h>

namespace DB
{

namespace Setting
{
    extern const SettingsString obfuscate_seed;
    extern const SettingsUInt64 obfuscate_markov_order;
    extern const SettingsUInt64 obfuscate_markov_frequency_cutoff;
    extern const SettingsUInt64 obfuscate_markov_num_buckets_cutoff;
    extern const SettingsUInt64 obfuscate_markov_frequency_add;
    extern const SettingsFloat obfuscate_markov_frequency_desaturate;
    extern const SettingsUInt64 obfuscate_markov_determinator_sliding_window_size;
}

std::pair<MarkovModelParameters, UInt64> ObfuscateStep::readParameters(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    const String & seed_string = settings[Setting::obfuscate_seed];
    UInt64 seed = seed_string.empty() ? randomSeed() : sipHash64(seed_string);

    MarkovModelParameters markov_model_params{};
    markov_model_params.order = settings[Setting::obfuscate_markov_order];
    markov_model_params.frequency_cutoff = settings[Setting::obfuscate_markov_frequency_cutoff];
    markov_model_params.num_buckets_cutoff = settings[Setting::obfuscate_markov_num_buckets_cutoff];
    markov_model_params.frequency_add = settings[Setting::obfuscate_markov_frequency_add];
    markov_model_params.frequency_desaturate = static_cast<double>(settings[Setting::obfuscate_markov_frequency_desaturate]);
    markov_model_params.determinator_sliding_window_size = settings[Setting::obfuscate_markov_determinator_sliding_window_size];

    markov_model_params.validate("Setting 'obfuscate_markov_order'", "Setting 'obfuscate_markov_frequency_desaturate'");

    return {markov_model_params, seed};
}

ObfuscateStep::ObfuscateStep(
    SharedHeader output_header_,
    ASTPtr inner_query_,
    Names column_names_,
    ContextPtr context_)
    : ISourceStep(std::move(output_header_))
    , inner_query(std::move(inner_query_))
    , column_names(std::move(column_names_))
    , context(std::move(context_))
{
    std::tie(markov_model_params, seed) = readParameters(context);

    /// Building the obfuscator picks a model per column, which is where an unsupported output type is
    /// rejected. Do it here, while the plan is built, rather than only when the pipeline is
    /// initialized: this is the earliest point at which the set of read columns is known, and an
    /// `obfuscate` over a column the obfuscator cannot model must not reach execution. The models are
    /// cheap and thrown away; the pipeline builds its own.
    Obfuscator validating_obfuscator(*getOutputHeader(), seed, markov_model_params);
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
