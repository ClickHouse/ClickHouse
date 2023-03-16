#pragma once

#include <optional>

#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>

namespace DB
{

class ArgumentExtractor
{
public:
    using CharArgument = std::optional<char>;
    using VectorArgument = std::vector<char>;
    using BoolArgument = std::optional<bool>;

    struct ParsedArguments
    {
        ColumnPtr data_column;

        CharArgument key_value_pair_delimiter = {};
        VectorArgument pair_delimiters = {};
        CharArgument quoting_character = {};
        BoolArgument with_escaping = {};
    };


    static ParsedArguments extract(const ColumnsWithTypeAndName & arguments);

private:
    static ArgumentExtractor::CharArgument extractControlCharacter(ColumnPtr column);
};

}
