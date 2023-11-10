#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

template <typename DataType>
std::optional<Int64> extractMaxSplitsImpl(const ColumnWithTypeAndName & argument)
{
    const auto * col = checkAndGetColumnConst<ColumnVector<DataType>>(argument.column.get());
    if (!col)
        return std::nullopt;

    auto value = col->template getValue<DataType>();
    return static_cast<Int64>(value);
}

std::optional<size_t> extractMaxSplits(const ColumnsWithTypeAndName & arguments, size_t max_substrings_argument_position)
{
    if (max_substrings_argument_position >= arguments.size())
        return std::nullopt;

    std::optional<Int64> max_splits;
    if (!((max_splits = extractMaxSplitsImpl<UInt8>(arguments[max_substrings_argument_position])) || (max_splits = extractMaxSplitsImpl<Int8>(arguments[max_substrings_argument_position]))
          || (max_splits = extractMaxSplitsImpl<UInt16>(arguments[max_substrings_argument_position])) || (max_splits = extractMaxSplitsImpl<Int16>(arguments[max_substrings_argument_position]))
          || (max_splits = extractMaxSplitsImpl<UInt32>(arguments[max_substrings_argument_position])) || (max_splits = extractMaxSplitsImpl<Int32>(arguments[max_substrings_argument_position]))
          || (max_splits = extractMaxSplitsImpl<UInt64>(arguments[max_substrings_argument_position])) || (max_splits = extractMaxSplitsImpl<Int64>(arguments[max_substrings_argument_position]))))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {}, which is {}-th argument",
            arguments[max_substrings_argument_position].column->getName(),
            max_substrings_argument_position + 1);

    if (*max_splits <= 0)
        return std::nullopt;

    return max_splits;
}

DataTypePtr FunctionArrayStringConcat::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors mandatory_args{
        {"arr", &isArray<IDataType>, nullptr, "Array"},
    };

    FunctionArgumentDescriptors optional_args{
        {"separator", &isString<IDataType>, isColumnConst, "const String"},
    };

    validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

    return std::make_shared<DataTypeString>();
}

REGISTER_FUNCTION(StringArray)
{
    factory.registerFunction<FunctionExtractAll>();

    factory.registerFunction<FunctionSplitByAlpha>();
    factory.registerAlias("splitByAlpha", FunctionSplitByAlpha::name);
    factory.registerFunction<FunctionSplitByNonAlpha>();
    factory.registerFunction<FunctionSplitByWhitespace>();
    factory.registerFunction<FunctionSplitByChar>();
    factory.registerFunction<FunctionSplitByString>();
    factory.registerFunction<FunctionSplitByRegexp>();
    factory.registerFunction<FunctionArrayStringConcat>();
}

}
