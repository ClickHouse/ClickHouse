#pragma once

#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <functional>
#include <mutex>
#include <optional>

namespace DB
{

struct Array;

Array getAggregateFunctionParametersArray(
    const ASTPtr & expression_list,
    const std::string & error_context,
    ContextPtr context);

void getAggregateFunctionNameAndParametersArray(
    const std::string & aggregate_function_name_with_params,
    std::string & aggregate_function_name,
    Array & aggregate_function_parameters,
    const std::string & error_context,
    ContextPtr context);

/// Remembers the result of `getAggregateFunctionNameAndParametersArray` for the functions that take the name of an
/// aggregate as a constant argument (`arrayReduce` and the like). Their resolver can be asked to build the function
/// again during the execution (by the adaptors for `Dynamic` and `Variant` arguments), when the context of the scope
/// it was created in may have expired. The name is parsed during the analysis, while the context is alive, and the
/// later builds with the same name reuse the result.
class CachedAggregateFunctionNameAndParameters
{
public:
    void get(
        const std::string & aggregate_function_name_with_params,
        std::string & aggregate_function_name,
        Array & aggregate_function_parameters,
        const std::string & error_context,
        const std::function<ContextPtr()> & get_context) const;

private:
    struct Parsed
    {
        std::string name_with_params;
        std::string name;
        Array parameters;
    };

    mutable std::mutex mutex;
    mutable std::optional<Parsed> parsed;
};

}
