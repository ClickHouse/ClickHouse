#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

class ASTAlterCommand;

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
/// Returns rewritten AST if any literals were wrapped, nullptr otherwise.
ASTPtr rewriteDateTimeLiteralsWithTimezone(
    const ASTAlterCommand & alter_command,
    const ColumnsDescription & columns,
    const String & session_timezone);

}
