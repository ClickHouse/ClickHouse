#pragma once

namespace DB
{

enum class ColumnAliasReplacementMode
{
    /// Used while analyzing queries. Replaced `ALIAS` columns keep the original
    /// result name and get an explicit type conversion for name resolution.
    QueryAnalysis,

    /// Used while analyzing skip index expressions. Replaced `ALIAS` columns are
    /// substituted by their bare expansion: neither a synthetic result name nor an
    /// explicit conversion to the declared alias type is added.
    ///
    /// Index granule columns are keyed by plain expression names, so a synthetic
    /// result name would never match what query analysis substitutes for a
    /// predicate over the alias.
    ///
    /// The type conversion is omitted on purpose, even though it means that for a
    /// narrowing alias type (`b UInt8 ALIAS a` over `a UInt16`) the index is built
    /// over the source domain rather than over the alias value. Skip index files
    /// are addressed by index name and carry no type information, so applying the
    /// conversion here would reinterpret the files of parts written before -
    /// including by earlier server versions, which accept such an index - as the
    /// narrower type, silently pruning granules that do match. Indexing the source
    /// expression can only lose pruning, never correctness.
    IndexAnalysis,
};

}
