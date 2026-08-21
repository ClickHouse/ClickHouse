#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace DB
{

class ColumnsDescription;

/// Dependency graph over the MATERIALIZED columns of a table: which top-level columns the default
/// expression of each of them reads, and whether it can be recomputed outside INSERT at all.
///
/// `AlterConversions` closes the read set of an on-fly read over the graph and `MutationsInterpreter`
/// resolves the same defaults, so the two must agree: a dependency they spell differently leaves the
/// pending mutation unapplied for that read task and returns the stale stored value.
class MaterializedColumnDependencies
{
public:
    struct Column
    {
        /// Subcolumn references already replaced by `getSubcolumn`. Clone before rewriting it.
        ASTPtr expression;
        Names dependencies;
        /// EPHEMERAL columns exist only during INSERT, so a column reading one is never recomputed
        /// afterwards and keeps its stored value.
        bool reads_ephemeral = false;
    };

    MaterializedColumnDependencies(const ColumnsDescription & columns, const ContextPtr & context);

    /// Null unless @column_name is a MATERIALIZED column with a default expression.
    const Column * tryGet(const String & column_name) const;

    /// Every MATERIALIZED column that has to be recomputed when @changed_columns change, including
    /// the ones reachable only through another recomputed one. A column @changed_columns names is
    /// included too when its own expression reads a changed column: the recompute overrides the
    /// assignment, and the read set has to cover it either way.
    NameSet getAffected(const NameSet & changed_columns) const;

private:
    std::unordered_map<String, Column> materialized_columns;
};

}
