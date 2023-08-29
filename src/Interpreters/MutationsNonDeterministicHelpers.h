#pragma once
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Core/Types.h>

namespace DB
{

struct MutationCommand;

struct FirstNonDeterministicFunctionResult
{
    std::optional<String> nondeterministic_function_name;
    bool subquery = false;
};

/// Searches for non-deterministic functions and subqueries which
/// may also be non-deterministic in expressions of mutation command.
FirstNonDeterministicFunctionResult findFirstNonDeterministicFunction(const MutationCommand & command, ContextPtr context);

}
