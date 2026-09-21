#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

namespace DB
{

class AccessRightsElements;
class IAST;
struct StorageInMemoryMetadata;

/// Appends a `SELECT` access requirement for the columns read by a mutation expression — a
/// `WHERE` predicate or the right-hand side of an `UPDATE` assignment. Evaluating such an
/// expression reads those columns, so it requires `SELECT` on them, exactly like a plain query
/// (otherwise their values could be inferred indirectly, or copied into a readable column).
///
/// Columns are resolved against `metadata` the same way a plain `SELECT` does: a name that is a
/// real column requires `SELECT` on it (qualification is stripped only when the result is not
/// itself a real column, so a real dotted name like `` `t.id` `` is preserved); a name that is a
/// virtual column not shadowed by a real one (e.g. `_part`, `_row_exists`) is skipped, since it is
/// not real data and needs no grant. A name on the right of `IN` that is not a column of this
/// table is a table or a set rather than a column, and is left to
/// `addExpressionIndirectReadsAccess`. Does nothing when `expression` is null.
void addExpressionColumnsSelectAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const String & database,
    const String & table,
    const StorageInMemoryMetadata & metadata);

/// Appends the access requirements for the reads a mutation expression performs indirectly, which
/// `addExpressionColumnsSelectAccess` cannot see: `RequiredSourceColumnsVisitor` stops at a
/// subquery, and the table a `dictGet` or `joinGet` reads is named by an argument rather than
/// referenced as a column. Covers a subquery (`... WHERE id IN (SELECT secret FROM other)`,
/// `... UPDATE visible = (SELECT secret FROM other)`), a table on the right of `IN`
/// (`... WHERE id IN other`), the `dictGet` family and `joinGet`.
///
/// The requirements are derived from names only, without resolving or analyzing anything, so they
/// hold when `validate_mutation_query = 0` defers validation because the objects do not exist yet -
/// and, unlike that validation, they cannot be turned off by a setting. A background mutation runs
/// with no user and therefore full access, so the submitting user's read access has to be
/// established here.
///
/// A subquery level that reads a single plain table and whose column references all attribute to it
/// produces a column-level requirement. Every other shape - a join, several tables, a nested
/// subquery or table function in `FROM`, an asterisk, a dotted name that may itself be a column -
/// falls back to requiring `SELECT` on the whole table, which is a superset and so never
/// under-requires. `WITH` names (within the `SELECT` that defines them and its subqueries) are not
/// tables to grant on and are skipped; a session temporary or external table of the same name is
/// not, because the background mutation that replays the expression has no such table and the name
/// can only be read there as a permanent one.
///
/// A table function - on the right of an `IN` or in the `FROM` of a subquery - requires the access
/// a call of it requires (`ITableFunction::getRequiredAccessForRead`), on the whole source of its
/// engine: the data it reads is named by its arguments rather than by an object to grant on, and
/// the instance that would check the access is otherwise built only by the background mutation,
/// under full access.
/// A query among its arguments (`view(SELECT ...)`) is analyzed like a subquery. A table function
/// whose reads depend on the grants of the current user (`ITableFunction::dependsOnCurrentUserGrants`,
/// e.g. `viewIfPermitted`) is refused with `BAD_ARGUMENTS` at any depth: the background mutation
/// executes it for no user, so no requirement would preserve the meaning it was checked with.
///
/// An unqualified table is required in `mutated_database` - the database the mutation expression is
/// qualified with before it is stored, and so the one it is read from - rather than in the session's
/// current database, and so is the object of a `dictGet` or `joinGet`, which the same visitor
/// qualifies with the same database.
/// `mutated_table` and `mutated_metadata` are used only to tell a table from a column on the right
/// of `IN`: `... WHERE x IN arr` reads an array column, `... WHERE x IN other` reads a table, and the
/// two are the same identifier in the AST. `mutated_metadata` may be null when the table is not
/// present locally, and then that case fails closed and requires the grant.
void addExpressionIndirectReadsAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const ContextPtr & context,
    const String & mutated_database,
    const String & mutated_table,
    const StorageInMemoryMetadata * mutated_metadata);

}
