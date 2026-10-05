#pragma once
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

struct MutationCommand;
class ASTAlterCommand;
class ColumnsDescription;

struct FirstNonDeterministicFunctionResult
{
    std::optional<String> nondeterministic_function_name;
    bool subquery = false;
};

/// Searches for non-deterministic functions and subqueries which
/// may also be non-deterministic in expressions of mutation command.
/// With `table_columns`, a function written without parentheses (`now`) is found as well.
FirstNonDeterministicFunctionResult findFirstNonDeterministicFunction(
    const MutationCommand & command, ContextPtr context, const ColumnsDescription * table_columns = nullptr);

/// Executes non-deterministic functions and subqueries in expressions of mutation
/// command and replaces them to the literals with a result of expressions.
/// Returns rewritten query if expressions were replaced, nullptr otherwise.
/// With `table_columns`, a function written without parentheses (`now`) is executed as well.
/// Names in `new_column_names` are treated as columns, names in `dropped_column_names` are not (unless also in `new_column_names`).
ASTPtr replaceNonDeterministicToScalars(
    const ASTAlterCommand & alter_command,
    ContextPtr context,
    const ColumnsDescription * table_columns = nullptr,
    const NameSet & new_column_names = {},
    const NameSet & dropped_column_names = {});

}
