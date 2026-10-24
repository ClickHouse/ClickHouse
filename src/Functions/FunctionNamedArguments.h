#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <base/defines.h>
#include <base/types.h>

#include <string_view>
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

    std::vector<Argument> arguments; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Throws if a name is not an identifier or is not unique, or if a required argument follows an optional one.
    void validate(const String & function_name) const;

    /// For each bound position, the index of the passed argument. `passed_names` is empty for positional ones.
    std::vector<size_t> bind(const String & function_name, const std::vector<String> & passed_names) const; // STYLE_CHECK_ALLOW_STD_CONTAINERS
};

void unknownFunctionArgumentName();

/// `FunctionNamedArguments` known at compile time, so the implementation reads arguments by name:
///     constexpr FunctionArgumentsSpec<2> spec = {{{"input"}, {"pattern", /*optional=*/ true}}};
///     constexpr size_t arg_pattern = spec.index("pattern");
template <size_t N>
struct FunctionArgumentsSpec
{
    struct Argument
    {
        std::string_view name;
        bool optional = false;
    };

    Argument arguments[N];

    /// A misspelled name does not compile.
    consteval size_t index(std::string_view name) const
    {
        for (size_t i = 0; i < N; ++i)
        {
            if (arguments[i].name == name)
                return i;
        }

        unknownFunctionArgumentName();
        return N;
    }

    const ColumnWithTypeAndName * tryGet(const ColumnsWithTypeAndName & passed_arguments, size_t index_) const
    {
        chassert(index_ < N);
        return index_ < passed_arguments.size() ? &passed_arguments[index_] : nullptr;
    }

    FunctionNamedArguments toNamedArguments() const
    {
        FunctionNamedArguments result;
        for (const auto & argument : arguments)
            result.arguments.push_back({String(argument.name), argument.optional});
        return result;
    }
};

}
