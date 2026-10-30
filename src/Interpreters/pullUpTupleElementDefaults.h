#pragma once

namespace DB
{

class ASTColumnDeclaration;

/// How the aggregate-function state versions are going to be pinned in the type of the column, which
/// has to be known for the `CAST` to a `Variant` alternative that the pull-up generates: a value is
/// converted to a `Variant` alternative only if the names of the types match exactly.
enum class TupleElementDefaultStateVersion
{
    /// The current state version is pinned (`pinCurrentStateVersionToAggregateFunctions`), as for a
    /// definition the user wrote in `CREATE TABLE` or `ALTER TABLE ... ADD COLUMN`.
    PinCurrent,
    /// An unversioned state is pinned to version 0 (`setVersionToAggregateFunctions`), as for a
    /// definition replayed from stored metadata, a backup or a distributed DDL entry.
    PinOldest,
    /// The versions are kept as written, as in `ALTER TABLE ... MODIFY COLUMN`.
    AsWritten,
};

/// DEFAULT expressions written inside a Tuple data type (e.g. `Tuple(a UInt8, s String DEFAULT 'Hello')`)
/// exist only at the syntax level. This pulls them up to the column level, normalizing the stored type to
/// one without any DEFAULTs and setting an equivalent column-level `DEFAULT tuple(...)` expression. Elements
/// without an explicit DEFAULT are filled with `defaultValueOfTypeName('<type>')`.
///
/// No-op for column declarations whose type contains no such DEFAULTs. Shared by the `CREATE TABLE` path and
/// the `ALTER TABLE ... ADD COLUMN` / `MODIFY COLUMN` path, so all of them normalize identically.
/// See https://github.com/ClickHouse/ClickHouse/issues/2797.
void pullUpTupleElementDefaults(ASTColumnDeclaration & col_decl, TupleElementDefaultStateVersion state_version);

}
