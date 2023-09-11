#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
