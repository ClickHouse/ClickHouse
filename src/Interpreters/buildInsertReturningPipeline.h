#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

class ASTInsertQuery;

/// Context with SETTINGS from the RETURNING subquery applied (for interpreters and result limits).
ContextMutablePtr makeReturningSelectContext(const ASTPtr & returning_select, ContextPtr context);

/// Wrap a completed INSERT pipeline so that the RETURNING SELECT runs after the INSERT finishes.
QueryPipeline buildInsertReturningPipeline(
    QueryPipeline insert_pipeline,
    const ASTPtr & returning_select,
    ContextPtr context);

/// Build a pulling pipeline for the RETURNING subquery only (used after native-protocol push inserts).
QueryPipeline buildReturningSelectPipeline(const ASTPtr & returning_select, ContextPtr context);

void setupPullingQueryPipeline(
    QueryPipeline & pipeline,
    ContextPtr context,
    QueryProcessingStage::Enum stage,
    const ASTPtr & returning_select = nullptr);

/// After a native-protocol push INSERT finishes, replace the pipeline with the RETURNING SELECT.
bool replacePipelineWithInsertReturningAfterPush(
    BlockIO & io,
    const ASTInsertQuery & insert_query,
    ContextPtr context,
    QueryProcessingStage::Enum stage);

}
