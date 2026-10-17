#include <Functions/FunctionNamedArguments.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>

#include <optional>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void unknownFunctionArgumentName()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown function argument name");
}

void FunctionNamedArguments::validate(const String & function_name) const
{
    std::unordered_set<std::string_view> names; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    bool has_optional = false;
    for (const auto & argument : arguments)
    {
        if (!isValidIdentifier(argument.name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument name '{}' of function {} is not an identifier", argument.name, function_name);
        if (!names.insert(argument.name).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument name '{}' of function {} is not unique", argument.name, function_name);
        if (has_optional && !argument.optional)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Required argument '{}' of function {} follows an optional argument", argument.name, function_name);
        has_optional |= argument.optional;
    }
}

std::vector<size_t> FunctionNamedArguments::bind(const String & function_name, const std::vector<String> & passed_names) const // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<std::optional<size_t>> positions(arguments.size()); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    bool has_named = false;

    for (size_t i = 0; i < passed_names.size(); ++i)
    {
        const auto & name = passed_names[i];
        size_t position = i;
        if (name.empty())
        {
            if (has_named)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Positional argument {} of function {} follows a named argument", i + 1, function_name);
            if (i >= arguments.size())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} takes at most {} arguments, passed {}", function_name, arguments.size(), passed_names.size());
        }
        else
        {
            has_named = true;
            auto it = std::find_if(arguments.begin(), arguments.end(), [&](const auto & argument) { return argument.name == name; });
            if (it == arguments.end())
            {
                String known_names;
                for (const auto & argument : arguments)
                    known_names += (known_names.empty() ? "" : ", ") + backQuoteIfNeed(argument.name);
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Function {} has no argument named {}. Its arguments are: {}", function_name, backQuoteIfNeed(name), known_names);
            }
            position = it - arguments.begin();
            if (positions[position])
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Argument {} of function {} is passed more than once", backQuoteIfNeed(name), function_name);
        }
        positions[position] = i;
    }

    size_t bound_size = 0;
    for (size_t position = 0; position < positions.size(); ++position)
        if (positions[position])
            bound_size = position + 1;

    std::vector<size_t> result; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    result.reserve(bound_size);
    for (size_t position = 0; position < positions.size(); ++position)
    {
        const auto & argument = arguments[position];
        if (positions[position])
            result.push_back(*positions[position]);
        else if (!argument.optional)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Argument {} of function {} is not passed", backQuoteIfNeed(argument.name), function_name);
        else if (position < bound_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Optional argument {} of function {} is omitted, but an argument after it is passed. "
                "An optional argument can be omitted only together with all the arguments after it",
                backQuoteIfNeed(argument.name), function_name);
    }

    return result;
}

}
