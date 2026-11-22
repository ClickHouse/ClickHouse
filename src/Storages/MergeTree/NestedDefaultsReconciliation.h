#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class ColumnsDescription;
struct StorageInMemoryMetadata;

/// Helpers shared by the writers that reconcile the `DEFAULT` of a missing array subcolumn of a
/// `Nested` structure with the offsets its siblings share (see
/// `reconcileEvaluatedDefaultWithSharedOffsets`): merges and the mutations that rewrite every column
/// of a part. The reconciliation may change the value the default expression produces, so everything
/// derived from that value before the part was rewritten (projections, skip indexes, stored
/// `MATERIALIZED` columns) has to be invalidated by the same rules in every such writer.

/// Collect the physical storage columns that a column's `DEFAULT` expression semantically
/// depends on. It uses the same scope-aware dependency analysis as default evaluation
/// (see `addDefaultRequiredExpressionsRecursively` / `evaluateMissingDefaults`):
/// `RequiredSourceColumnsVisitor` correctly ignores lambda formal parameters, so
/// `arrayMap(x -> f(x), col)` depends on `col`, not on a column named `x`. Each source column is
/// resolved to its storage column name (e.g. the subcolumn `m.keys` resolves to the `Map` column
/// `m`), including subcolumns read through an alias (e.g. `m_alias.keys` with `m_alias ALIAS m`
/// resolves to `m`).
///
/// The dependency closure is transitive through intermediate columns that have no values in any
/// source, so that a default depending on an expired column *through* another column is still
/// detected:
/// - `ALIAS` columns are never physically stored, so they are always expanded into the physical
///   columns their expression reads (and are not themselves reported).
/// - An ordinary `DEFAULT`/`MATERIALIZED` column that is recomputed from its expression for the
///   rows of every source part during the merge has its dependencies expanded too (and the column
///   itself is still reported). A chain such as `tmp DEFAULT m.keys`, `n.b DEFAULT tmp` with `m`
///   expired therefore reports `m`, so `n.b` is expired as well.
/// `columns_to_follow` holds such intermediates. The recursion stops at an intermediate that the merge
/// is forced to write anyway: one stored in some source part, omitted with a `MissingColumnInfo`
/// marker in some part, backed by a patch part, or being a pending rename target. The merge computes
/// that intermediate, computes the outer default from it, and reconciles the outer value with the
/// shared `Nested` offsets (see `reconcileEvaluatedDefaultWithSharedOffsets`), so the outer column
/// can be materialized safely. Expiring it instead would turn a merge-time value into a read-time
/// one, which changes the data for a non-deterministic default.
NameSet collectDefaultStorageDependencies(
    const ASTPtr & default_expression,
    const ColumnsDescription & columns_desc,
    const NameSet & columns_to_follow);

/// Array subcolumns of `Nested` structures with shared offsets that some source part does not store
/// while storing another array subcolumn of the same structure. Reading such a part reconciles the
/// default of the missing subcolumn with the offsets the part stores, which may change the value the
/// default expression produces, so the data derived from that value before the part is rewritten may
/// disagree with the new part.
NameSet getNestedColumnsReconciledInSomePart(const NamesAndTypesList & storage_columns, const MergeTreeData::DataPartsVector & parts);

/// Whether an expression with these required columns (of a projection or a skip index) may read a
/// column of @reconciled_columns, directly or through the default expression of a column it reads.
bool mayReadReconciledNestedDefault(
    const Names & required_columns,
    const NameSet & reconciled_columns,
    const ColumnsDescription & columns_desc,
    const NameSet & storage_column_names);

/// Whether the stored values of the column may be computed again from its expression when a column
/// the expression reads is reconciled: it is a `MATERIALIZED` column, the expression is deterministic
/// after the `ALIAS` columns it reads are expanded (`s MATERIALIZED length(n.b) + r` with
/// `r ALIAS rand64()` is not), it reads no `EPHEMERAL` column, and the column takes no part in the
/// partition, sorting or primary key: a new value would not move the row to another partition or
/// to another position in the part.
bool isStoredMaterializedColumnRecomputable(
    const String & column_name,
    const ColumnsDescription & columns_desc,
    const StorageInMemoryMetadata & metadata,
    const ContextPtr & context);

}
