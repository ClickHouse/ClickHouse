#include <Functions/keyvaluepair/ArgumentExtractor.h>

#include <Functions/FunctionHelpers.h>

#include <DataTypes/DataTypeString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    auto popFrontAndGet(auto & container)
    {
        auto element = container.front();
        container.pop_front();
        return element;
    }
}

ArgumentExtractor::ParsedArguments ArgumentExtractor::extract(const ColumnsWithTypeAndName & arguments)
{
    return extract(ColumnsWithTypeAndNameList{arguments.begin(), arguments.end()});
}

ArgumentExtractor::ParsedArguments ArgumentExtractor::extract(ColumnsWithTypeAndNameList arguments)
{
    static constexpr auto MAX_NUMBER_OF_ARGUMENTS = 4u;

    if (arguments.empty() || arguments.size() > MAX_NUMBER_OF_ARGUMENTS)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Function extractKeyValuePairs requires at least 1 argument and at most {}. {} was provided",
                        MAX_NUMBER_OF_ARGUMENTS, arguments.size());
    }

    auto data_column = extractStringColumn(popFrontAndGet(arguments), "data_column");

    if (arguments.empty())
    {
        return ParsedArguments{data_column};
    }

    auto key_value_delimiter = extractSingleCharacter(popFrontAndGet(arguments), "key_value_delimiter");

    if (arguments.empty())
    {
        return ParsedArguments {data_column, key_value_delimiter};
    }

    auto pair_delimiters = extractVector(popFrontAndGet(arguments), "pair_delimiters");

    if (arguments.empty())
    {
        return ParsedArguments {
            data_column, key_value_delimiter, pair_delimiters
        };
    }

    auto quoting_character = extractSingleCharacter(popFrontAndGet(arguments), "quoting_character");

    return ParsedArguments {
        data_column,
        key_value_delimiter,
        pair_delimiters,
        quoting_character,
    };
}

ArgumentExtractor::CharArgument ArgumentExtractor::extractSingleCharacter(const ColumnWithTypeAndName & argument, const std::string & parameter_name)
{
    const auto type = argument.type;
    const auto column = argument.column;

    validateColumnType(type, parameter_name);

    auto view = column->getDataAt(0).toView();

    if (view.empty())
    {
        return {};
    }
    if (view.size() == 1u)
    {
        return view.front();
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Control character argument must either be empty or contain exactly 1 character");
}

ColumnPtr ArgumentExtractor::extractStringColumn(const ColumnWithTypeAndName & argument, const std::string & parameter_name)
{
    auto type = argument.type;
    auto column = argument.column;

    validateColumnType(type, parameter_name);

    return column;
}

ArgumentExtractor::VectorArgument ArgumentExtractor::extractVector(const ColumnWithTypeAndName & argument, const std::string & parameter_name)
{
    const auto type = argument.type;
    const auto column = argument.column;

    validateColumnType(type, parameter_name);

    auto view = column->getDataAt(0).toView();

    return {view.begin(), view.end()};
}

void ArgumentExtractor::validateColumnType(DataTypePtr type, const std::string & parameter_name)
{
    if (!isStringOrFixedString(type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument {}. Must be String.",
            type, parameter_name);
    }
}

}
