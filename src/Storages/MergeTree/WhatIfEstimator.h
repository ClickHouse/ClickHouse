#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/WhatIfResult.h>

namespace DB
{

/// estimates the hypothetical indexes and projections of the session against the baseline read, for `EXPLAIN WHATIF`
WhatIfResult estimateHypotheticalObjects(const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings);

}
