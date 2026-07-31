#pragma once

namespace DB
{

enum class ColumnAliasReplacementMode
{
    /// Used while analyzing queries. Replaced `ALIAS` columns keep the original
    /// result name and get an explicit type conversion for name resolution.
    QueryAnalysis,

    /// Used while analyzing skip index expressions. Replaced `ALIAS` columns get
    /// the explicit type conversion (the index must store the alias value, whose
    /// domain differs from the source expression for narrowing alias types), but
    /// no synthetic result names: index granule columns are keyed by plain
    /// expression names, which is what query analysis substitutes for predicates
    /// over the alias when matching the index.
    IndexAnalysis,
};

}
