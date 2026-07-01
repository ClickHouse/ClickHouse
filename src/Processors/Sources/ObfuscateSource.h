#pragma once

#include <memory>
#include <Common/Obfuscator/Obfuscator.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class PullingPipelineExecutor;

/** Source that runs an inner SELECT query twice:
  *   1. Training phase: drains the inner query to train the obfuscator's models.
  *   2. Generation phase: re-executes the inner query repeatedly, feeding each
  *      block through the obfuscator to produce anonymized output.
  * On each exhaustion of the inner query during generation, the obfuscator seed
  * is advanced and a fresh pipeline is built, so the stream is effectively
  * infinite and bounded only by an outer LIMIT.
  */
class ObfuscateSource : public ISource
{
public:
    ObfuscateSource(
        SharedHeader header_,
        ASTPtr inner_query_,
        Names column_names_,
        ContextPtr context_,
        MarkovModelParameters markov_model_params_,
        UInt64 seed_);

    ~ObfuscateSource() override;

    String getName() const override { return "ObfuscateSource"; }

protected:
    Chunk generate() override;

private:
    void rebuildInnerPipeline();

    ASTPtr inner_query;
    Names column_names;
    ContextPtr context;
    Obfuscator obfuscator;

    enum class Phase : uint8_t
    {
        Training,
        Generating,
    };

    Phase phase = Phase::Training;
    bool source_was_empty = true;
    /// Whether the current generation pass over the inner query has produced any rows.
    /// If a full pass yields nothing, we stop instead of rebuilding forever.
    bool generated_rows_in_pass = false;

    QueryPipeline inner_pipeline;
    std::unique_ptr<PullingPipelineExecutor> inner_executor;
};

}
