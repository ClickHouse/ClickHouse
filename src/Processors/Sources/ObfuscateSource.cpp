#include <Processors/Sources/ObfuscateSource.h>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 limit;
    extern const SettingsUInt64 offset;
}

namespace
{

/// The inner query is interpreted as a standalone top-level SELECT, so the query-level
/// `limit` / `offset` result settings would be applied to it as well. That would train and
/// generate from a truncated source just because the user limited the final result, e.g.
/// `SELECT * FROM obfuscate(SELECT * FROM numbers(1000)) SETTINGS limit = 10`. Clear them
/// for the inner execution; the outer pipeline still applies them to the obfuscated output.
ContextPtr makeInnerContext(const ContextPtr & context)
{
    if (context->getSettingsRef()[Setting::limit] == 0 && context->getSettingsRef()[Setting::offset] == 0)
        return context;

    auto inner_context = Context::createCopy(context);
    inner_context->setSetting("limit", Field(UInt64(0)));
    inner_context->setSetting("offset", Field(UInt64(0)));
    return inner_context;
}

}

ObfuscateSource::ObfuscateSource(
    SharedHeader header_,
    ASTPtr inner_query_,
    Names column_names_,
    ContextPtr context_,
    MarkovModelParameters markov_model_params_,
    UInt64 seed_)
    : ISource(header_)
    , inner_query(std::move(inner_query_))
    , column_names(std::move(column_names_))
    , context(makeInnerContext(context_))
    , obfuscator(*header_, seed_, markov_model_params_)
{
}

ObfuscateSource::~ObfuscateSource() = default;

void ObfuscateSource::rebuildInnerPipeline()
{
    inner_executor.reset();
    inner_pipeline.reset();

    SelectQueryOptions options(QueryProcessingStage::Complete);

    QueryPipelineBuilder builder;
    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(inner_query, context, options, column_names);
        builder = interpreter.buildQueryPipeline();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter(inner_query, context, options, column_names);
        builder = interpreter.buildQueryPipeline();
    }

    /// The obfuscator expects non-constant columns. Materialize the inner stream
    /// to drop any constant/sparse representations (mirrors what StorageView does
    /// via an ExpressionStep in the outer plan).
    builder.addSimpleTransform([](const SharedHeader & cur_header)
    {
        return std::make_shared<MaterializingTransform>(cur_header);
    });

    inner_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    inner_executor = std::make_unique<PullingPipelineExecutor>(inner_pipeline);
}

Chunk ObfuscateSource::generate()
{
    if (phase == Phase::Training)
    {
        rebuildInnerPipeline();

        Block block;
        while (inner_executor->pull(block))
        {
            if (block.rows() == 0)
                continue;
            source_was_empty = false;
            obfuscator.train(block.getColumns());
        }

        obfuscator.finalize();
        inner_executor.reset();
        inner_pipeline.reset();

        phase = Phase::Generating;

        /// If the inner query produced no rows there is nothing to obfuscate;
        /// signal EOF to the outer pipeline by returning an empty chunk.
        if (source_was_empty)
            return {};
    }

    while (true)
    {
        if (!inner_executor)
        {
            rebuildInnerPipeline();
            generated_rows_in_pass = false;
        }

        Block block;
        if (inner_executor->pull(block))
        {
            if (block.rows() == 0)
                continue;

            generated_rows_in_pass = true;
            Columns columns = obfuscator.generate(block.getColumns());
            size_t num_rows = block.rows();
            return Chunk(std::move(columns), num_rows);
        }

        /// Inner pipeline drained.
        inner_executor.reset();
        inner_pipeline.reset();

        /// Fail closed: if a full generation pass produced no rows (e.g. the inner query is
        /// non-repeatable and became empty after a non-empty training pass), stop instead of
        /// rebuilding forever. Otherwise advance the seed and produce a fresh stream of source
        /// blocks for the next iteration. The outer LIMIT bounds the otherwise-infinite stream.
        if (!generated_rows_in_pass)
            return {};

        obfuscator.updateSeed();
    }
}

}
