#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <optional>
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
///
/// Analysing a default costs a `TreeRewriter` run, and this graph is built per read task per part, so
/// nothing is analysed until it is asked for.
///
/// Must not outlive the `ColumnsDescription` it was built from.
class MaterializedColumnDependencies
{
public:
    struct MaterializedDependencyNode
    {
        /// ALIAS references expanded and subcolumn references replaced by `getSubcolumn`.
        /// Clone before rewriting it.
        ASTPtr expression;
        Names dependencies;
        /// EPHEMERAL columns exist only during INSERT, so a column reading one is never recomputed
        /// afterwards and keeps its stored value.
        bool reads_ephemeral = false;
    };

    MaterializedColumnDependencies(const ColumnsDescription & columns_, const ContextPtr & context_);

    /// The node of @column_name: the expression to evaluate and the columns it reads directly.
    /// Null unless @column_name is a MATERIALIZED column with a default expression.
    const MaterializedDependencyNode * findNode(const String & column_name) const;

    /// The columns that have to be read to recalculate @column_name when @changed_columns are updated.
    /// Empty unless @column_name is a MATERIALIZED column that has to be recalculated — because its
    /// own expression reads one of @changed_columns, or because it reads another column that has to
    /// be recalculated. Also empty for a column reading an EPHEMERAL one, which is never recalculated.
    /// Answers are memoised for one @changed_columns set; passing a different one starts over.
    const Names & findColumnsToRecalculate(const String & column_name, const NameSet & changed_columns) const;

private:
    /// The memo must already be keyed on @changed_columns; the only walk starts above.
    bool willBeRecalculated(const String & column_name, const NameSet & changed_columns) const;

    const NamesAndTypesList & getSourceColumns() const;

    const ColumnsDescription & columns;
    ContextPtr context;

    /// Every MATERIALIZED column of the table. An entry is created empty and filled in on first use;
    /// a set `expression` is what marks it analysed.
    mutable std::unordered_map<String, MaterializedDependencyNode> materialized_columns;

    /// Physical columns plus EPHEMERAL ones, built on the first analysis.
    mutable std::optional<NamesAndTypesList> source_columns;
    mutable NameSet ephemeral_columns;

    mutable std::optional<NameSet> memo_changed_columns;
    mutable std::unordered_map<String, bool> will_be_recalculated;
};

}
