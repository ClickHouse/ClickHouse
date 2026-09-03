#pragma once

#include <Common/Obfuscator/Obfuscator.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

class ObfuscateStep : public ISourceStep
{
public:
    ObfuscateStep(
        SharedHeader output_header_,
        ASTPtr inner_query_,
        Names column_names_,
        ContextPtr context_);

    String getName() const override { return "Obfuscate"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    ASTPtr inner_query;
    Names column_names;
    ContextPtr context;
    MarkovModelParameters markov_model_params{};
    UInt64 seed = 0;
};

}
