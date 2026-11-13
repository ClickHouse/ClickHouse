#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ASTAlterCommand;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/// When `session_timezone` is set, string literals compared to DateTime/DateTime64
/// columns in mutation predicates must be interpreted in that timezone.
/// However, mutations execute in a background thread that lacks the original
/// session context. This function rewrites the mutation AST at ALTER time:
///
///   time >= '2000-01-01 02:00:00'
///     becomes
///   time >= toDateTime('2000-01-01 02:00:00', 'America/Mazatlan')
///
/// This makes the timezone explicit so the background thread evaluates it correctly.
/// The expressions are analyzed against the table with the settings of the context, so that a literal is converted with the
/// type of what the analyzer binds the identifier it is compared with to: a column, a subcolumn, an alias, or a parameter of a
/// lambda, in its scope.
/// Returns rewritten AST if any literals were wrapped, nullptr otherwise.
ASTPtr rewriteDateTimeLiteralsWithTimezone(
    const ASTAlterCommand & alter_command,
    const StoragePtr & table,
    const StorageSnapshotPtr & storage_snapshot,
    const String & session_timezone,
    const ContextPtr & context);

/// The same for a `TTL` rewritten in place, which is evaluated by background merges without the session either. Its
/// expressions are analyzed against a table with the columns `columns`. Returns whether any literal was wrapped.
bool rewriteDateTimeLiteralsWithTimezone(
    const ASTPtr & expression, const ColumnsDescription & columns, const String & session_timezone, const ContextPtr & context);

}
