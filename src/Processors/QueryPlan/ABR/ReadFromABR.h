#pragma once

#include <Common/AtomicLogger.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/ABR/StorageABR.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

/// Query plan step that executes the ABR query on a source table and retries
/// on the next coarser table if a fallback error (TOO_MANY_ROWS, TOO_MANY_BYTES, TIMEOUT_EXCEEDED)
/// is thrown. Results are buffered (memory → disk cascade) before being emitted so
/// that a failed attempt's partial output is never visible to the client.
class ReadFromABR : public ISourceStep
{
public:
    /// `tables_` are the tables to try in order, from the chosen one to the coarsest candidate.
    /// The step keeps them alive until it is executed.
    /// initial_attempt_ holds the interpreter already built in StorageABR::read()
    /// for the first table. It is reused on the first attempt so the analysis pass
    /// that computed the result header is not repeated. Retries (after a fallback
    /// error) build a fresh interpreter for the next coarser table.
    ReadFromABR(
        const SelectQueryInfo & query_info_,
        std::vector<StorageABR::SourceTable> tables_,
        StorageABR::Attempt initial_attempt_,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage_,
        LoggerPtr logger_);

    String getName() const override { return "ReadFromABR"; }
    static void addConvertingActions(Pipe & pipe, const Block & header, const ContextPtr & context);
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    /// Holds the buffered output of a completed query attempt.
    struct BufferedResult
    {
        std::shared_ptr<ISource> replay_source;
        Block totals;
        Block extremes;
    };

    /// Runs the pipeline of an attempt to completion, buffering all output via CascadeWriteBuffer.
    /// `context` is the context of the attempt, whose `max_execution_time` is enforced for the whole execution.
    /// Static so it can be called from the deferred DelayedSource creator without depending on the (already-destroyed) step instance.
    static BufferedResult executeAndBuffer(QueryPipeline & pipeline, const ContextPtr & context, QueryProcessingStage::Enum processed_stage);

    /// Attaches totals/extremes from a buffered result to the output pipe.
    static void addTotalsAndExtremes(Pipe & pipe, const BufferedResult & buf_result);

    SelectQueryInfo query_info;
    std::vector<StorageABR::SourceTable> tables;
    /// Pre-built attempt for the first table, consumed by the first attempt in `initializePipeline`.
    StorageABR::Attempt initial_attempt;
    ContextPtr context;
    QueryProcessingStage::Enum processed_stage;
    AtomicLogger logger;
};

}
