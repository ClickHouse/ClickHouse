#pragma once

namespace DB
{

enum class ColumnAliasReplacementMode
{
    /// Used while analyzing queries. Replaced `ALIAS` columns keep the original
    /// result name and get an explicit type conversion for name resolution.
    QueryAnalysis,

    /// Used while normalizing metadata ASTs for persistence. Replaced `ALIAS`
    /// columns are written as plain expressions without synthetic result names
    /// or type conversions.
    MetadataNormalization,
};

}
