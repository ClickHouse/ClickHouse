#include "ArgumentExtractor.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

ArgumentExtractor::ParsedArguments ArgumentExtractor::extract(const ColumnsWithTypeAndName & arguments)
{
    /*
     * TODO validate arguments:
     *  1. Check if argument is one of the acceptable characters for that argument
     *  2. Check if it's not empty
     *  3. Cross check arguments? Not sure it is needed anymore
     *  4. Use uint8_t column instead of string column for escaping lol
     *  5. maybe a builder will clean things up here
     *  6. Refactor exceptions so it's re-thrown on API side
     * */

    if (arguments.empty())
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function extractKeyValuePairs requires at least one argument");
    }

    auto data_column = arguments[0].column;

    if (arguments.size() == 1u)
    {
        return ParsedArguments{data_column};
    }

    auto key_value_pair_delimiter = extractControlCharacter(arguments[1].column);

    if (arguments.size() == 2u)
    {
        return ParsedArguments {data_column, key_value_pair_delimiter};
    }

    auto pair_delimiters_characters = arguments[2].column->getDataAt(0).toView();

    VectorArgument pair_delimiters {pair_delimiters_characters.begin(), pair_delimiters_characters.end()};

    if (arguments.size() == 3u)
    {
        return ParsedArguments {
            data_column, key_value_pair_delimiter, pair_delimiters
        };
    }

    auto quoting_character = extractControlCharacter(arguments[3].column);

    if (arguments.size() == 4u)
    {
        return ParsedArguments {
            data_column,
            key_value_pair_delimiter,
            pair_delimiters,
            quoting_character,
        };
    }

    auto with_escaping_character = extractControlCharacter(arguments[4].column);

    bool with_escaping = with_escaping_character && with_escaping_character == '1';

    return ParsedArguments {
        data_column, key_value_pair_delimiter, pair_delimiters, quoting_character, with_escaping
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

}

