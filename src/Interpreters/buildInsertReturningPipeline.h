#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

/// Wrap a completed INSERT pipeline so that the RETURNING SELECT runs after the INSERT finishes.
QueryPipeline buildInsertReturningPipeline(
    QueryPipeline insert_pipeline,
    const ASTPtr & returning_select,
    ContextPtr context);

}
