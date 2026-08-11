#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/WhatIfResult.h>

namespace DB
{

/// Estimates the benefit of hypothetical skip indexes over the baseline
/// (after PK + partition + existing index pruning). Used by EXPLAIN WHATIF
class WhatIfIndexEstimator
{
public:
    static WhatIfResult run(const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings);
};

}
