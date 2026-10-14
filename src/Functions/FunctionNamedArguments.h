#pragma once

#include <base/types.h>

#include <vector>


namespace DB
{

/// Arguments that can be passed by name `f(name => value)`, the analyzer binds them to positions.
struct FunctionNamedArguments
{
    struct Argument
    {
        String name;
        /// An optional argument can be omitted only together with all the arguments after it.
        bool optional = false;
    };

    std::vector<Argument> arguments;

    /// Throws if a name is not an identifier or is not unique, or if a required argument follows an optional one.
    void validate(const String & function_name) const;

    /// For each bound position, the index of the passed argument. `passed_names` is empty for positional ones.
    std::vector<size_t> bind(const String & function_name, const std::vector<String> & passed_names) const;
};

}
