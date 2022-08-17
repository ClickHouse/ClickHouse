#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


std::vector<FunctionJSONHelpers::Move> FunctionJSONHelpers::prepareMoves(const char * function_name, const ColumnsWithTypeAndName & columns, size_t first_index_argument, size_t num_index_arguments)
{
    std::vector<Move> moves;
    moves.reserve(num_index_arguments);
    for (const auto i : collections::range(first_index_argument, first_index_argument + num_index_arguments))
    {
        const auto & column = columns[i];
        if (!isString(column.type) && !isInteger(column.type))
            throw Exception{"The argument " + std::to_string(i + 1) + " of function " + String(function_name)
                                + " should be a string specifying key or an integer specifying index, illegal type: " + column.type->getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (column.column && isColumnConst(*column.column))
        {
            const auto & column_const = assert_cast<const ColumnConst &>(*column.column);
            if (isString(column.type))
                moves.emplace_back(MoveType::ConstKey, column_const.getValue<String>());
            else
                moves.emplace_back(MoveType::ConstIndex, column_const.getInt(0));
        }
        else
        {
            if (isString(column.type))
                moves.emplace_back(MoveType::Key, "");
            else
                moves.emplace_back(MoveType::Index, 0);
        }
    }
    return moves;
}

size_t FunctionJSONHelpers::calculateMaxSize(const ColumnString::Offsets & offsets)
{
    size_t max_size = 0;
    for (const auto i : collections::range(0, offsets.size()))
    {
        size_t size = offsets[i] - offsets[i - 1];
        if (max_size < size)
            max_size = size;
    }
    if (max_size)
        --max_size;
    return max_size;
}


void registerFunctionsJSON(FunctionFactory & factory)
{
    factory.registerFunction<FunctionJSON<NameJSONHas, JSONHasImpl>>();
    factory.registerFunction<FunctionJSON<NameIsValidJSON, IsValidJSONImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONLength, JSONLengthImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONKey, JSONKeyImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONType, JSONTypeImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtract, JSONExtractImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractArrayRaw, JSONExtractArrayRawImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValuesRaw, JSONExtractKeysAndValuesRawImpl>>();
}

}
