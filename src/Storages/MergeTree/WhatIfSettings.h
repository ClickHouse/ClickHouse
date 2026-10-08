#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Settings of the `EXPLAIN WHATIF` query itself (the `SETTINGS` clause of the EXPLAIN)
struct WhatIfSettings
{
    bool empirical = true;
    /// row budget for projection estimates, 0 means no limit
    UInt64 projection_scan_budget_rows = 10'000'000;

    static WhatIfSettings fromAST(const ASTPtr & settings_ast);
};

}
