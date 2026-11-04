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
/// The context gives the higher-order functions that bind the types of the parameters of a lambda in the expression. The
/// types of the aliases in the command are those the analyzer gives them against the table.
/// Returns rewritten AST if any literals were wrapped, nullptr otherwise.
ASTPtr rewriteDateTimeLiteralsWithTimezone(
    const ASTAlterCommand & alter_command,
    const StoragePtr & table,
    const StorageSnapshotPtr & storage_snapshot,
    const String & session_timezone,
    const ContextPtr & context);

/// The same for one expression rewritten in place, such as a `TTL`, which is evaluated by background merges without the
/// session either. Returns whether any literal was wrapped.
bool rewriteDateTimeLiteralsWithTimezone(
    const ASTPtr & expression, const ColumnsDescription & columns, const String & session_timezone, const ContextPtr & context);

}
