#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <list>
#include <optional>

namespace DB
{

/*
 * Validate (API level validation, no business logic validation) and extracts input arguments from
 * `ColumnsWithTypeAndName` into ArgumentExtractor::ParsedArguments.
 * */
class ArgumentExtractor
{
public:
    using CharArgument = std::optional<char>;
    using VectorArgument = std::vector<char>;
    using ColumnsWithTypeAndNameList = std::list<ColumnWithTypeAndName>;

    struct ParsedArguments
    {
        ColumnPtr data_column;

        CharArgument key_value_delimiter = {};
        VectorArgument pair_delimiters = {};
        CharArgument quoting_character = {};
    };


    static ParsedArguments extract(const ColumnsWithTypeAndName & arguments);
    static ParsedArguments extract(ColumnsWithTypeAndNameList arguments);

private:
    static CharArgument extractSingleCharacter(const ColumnWithTypeAndName & arguments, const std::string & parameter_name);
    static ColumnPtr extractStringColumn(const ColumnWithTypeAndName & arguments, const std::string & parameter_name);
    static VectorArgument extractVector(const ColumnWithTypeAndName & arguments, const std::string & parameter_name);

    static void validateColumnType(DataTypePtr type, const std::string & parameter_name);
};

}
