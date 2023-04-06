#include <Functions/keyvaluepair/ArgumentExtractor.h>

#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

ArgumentExtractor::ParsedArguments ArgumentExtractor::extract(const ColumnsWithTypeAndName & arguments)
{
    if (arguments.empty())
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function extractKeyValuePairs requires at least one argument");
    }

    auto data_column = arguments[0].column;

    if (arguments.size() == 1u)
    {
        return ParsedArguments{data_column};
    }

    auto key_value_delimiter = extractControlCharacter(arguments[1].column);

    if (arguments.size() == 2u)
    {
        return ParsedArguments {data_column, key_value_delimiter};
    }

    auto pair_delimiters_characters = arguments[2].column->getDataAt(0).toView();

    VectorArgument pair_delimiters {pair_delimiters_characters.begin(), pair_delimiters_characters.end()};

    if (arguments.size() == 3u)
    {
        return ParsedArguments {
            data_column, key_value_delimiter, pair_delimiters
        };
    }

    auto quoting_character = extractControlCharacter(arguments[3].column);

    if (arguments.size() == 4u)
    {
        return ParsedArguments {
            data_column,
            key_value_delimiter,
            pair_delimiters,
            quoting_character,
        };
    }

    auto with_escaping = extractBoolArgument(arguments[4].column);

    return ParsedArguments {
        data_column, key_value_delimiter, pair_delimiters, quoting_character, with_escaping
    };
}

ArgumentExtractor::CharArgument ArgumentExtractor::extractControlCharacter(ColumnPtr column)
{
    auto view = column->getDataAt(0).toView();

    if (view.empty())
    {
        return {};
    }
    else if (view.size() == 1u)
    {
        return view.front();
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Control character argument must either be empty or contain exactly 1 character");
}

ArgumentExtractor::BoolArgument ArgumentExtractor::extractBoolArgument(ColumnPtr column)
{
    if (const auto * escaping_support_column = checkAndGetColumnConst<ColumnUInt8>(column.get()))
    {
        if (escaping_support_column->empty())
        {
            return {};
        }
        else if (escaping_support_column->size() == 1u)
        {
            return escaping_support_column->getBool(0u);
        }
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Boolean argument must either be empty or contain exactly 1 value");
}

}

