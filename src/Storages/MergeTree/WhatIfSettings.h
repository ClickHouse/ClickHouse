#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Settings of the `EXPLAIN WHATIF` query itself (the `SETTINGS` clause of the EXPLAIN)
struct WhatIfSettings
{
    bool empirical = true;
    /// rows a projection estimate may read before it samples granules instead, 0 reads the parts whole
    UInt64 max_rows_to_scan = 10'000'000;

    static WhatIfSettings fromAST(const ASTPtr & settings_ast);
};

}
