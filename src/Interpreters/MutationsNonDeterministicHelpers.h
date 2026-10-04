#pragma once
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Core/Types.h>
#include <Core/Names.h>

namespace DB
{

struct MutationCommand;
struct StorageID;
class ASTAlterCommand;

struct FirstNonDeterministicFunctionResult
{
    std::optional<String> nondeterministic_function_name;
    /// A virtual column declared with `deterministic = false` (for example `_table`, `_database`
    /// or `_disk_name`), whose value may differ between replicas of the same table.
    std::optional<String> nondeterministic_virtual_column_name;
    bool subquery = false;
};

/// Searches for non-deterministic functions and subqueries which
/// may also be non-deterministic in expressions of mutation command.
/// Identifiers found in `nondeterministic_virtual_columns` are reported as non-deterministic
/// virtual columns; pass the virtual columns of the storage that are declared non-deterministic
/// and are not shadowed by a real column of the table. When `storage_id` of the mutated table is
/// given, identifiers qualified with it (`t._table`, `db.t._table`) are matched by their short name.
/// Lambda parameters shadow the virtual columns with the same names inside the lambda body.
FirstNonDeterministicFunctionResult findFirstNonDeterministicFunction(
    const MutationCommand & command,
    ContextPtr context,
    const NameSet & nondeterministic_virtual_columns = {},
    const StorageID * storage_id = nullptr);

/// Executes non-deterministic functions and subqueries in expressions of mutation
/// command and replaces them to the literals with a result of expressions.
/// Returns rewritten query if expressions were replaced, nullptr otherwise.
ASTPtr replaceNonDeterministicToScalars(const ASTAlterCommand & alter_command, ContextPtr context);

}
