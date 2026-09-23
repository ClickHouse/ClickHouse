#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Settings of the `EXPLAIN WHATIF` query itself (the `SETTINGS` clause of the EXPLAIN)
struct WhatIfSettings
{
    bool empirical = true;

    static WhatIfSettings fromAST(const ASTPtr & settings_ast);
};

}
