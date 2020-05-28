#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/EarlyWindowFunction.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>


#include <ext/range.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <template <typename, typename> class GeneralData, template <typename> class StringData, typename T>
AggregateFunctionPtr createEarlyWindowFunctionSecondArg(const std::string & name, const DataTypes & arguments, const Array & params)
{
    WhichDataType which(*arguments[1]);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<EarlyWindowFunction<T, GeneralData<T, TYPE>>>(arguments, params);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return std::make_shared<EarlyWindowFunction<T, GeneralData<T, Int8>>>(arguments, params);
    if (which.idx == TypeIndex::Enum16) return std::make_shared<EarlyWindowFunction<T, GeneralData<T, Int16>>>(arguments, params);
    if (which.idx == TypeIndex::Date) return std::make_shared<EarlyWindowFunction<T, GeneralData<T, UInt16>>>(arguments, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<EarlyWindowFunction<T, GeneralData<T, UInt32>>>(arguments, params);
    if (which.idx == TypeIndex::String) return std::make_shared<EarlyWindowFunction<T, StringData<T>>>(arguments, params);
    if (which.idx == TypeIndex::FixedString) return std::make_shared<EarlyWindowFunction<T, StringData<T>>>(arguments, params);

    throw Exception{"Illegal type " + arguments[1].get()->getName() + " of second argument of aggregate function " + name,
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

template <template <typename, typename> class GeneralData, template <typename> class StringData>
AggregateFunctionPtr createEarlyWindowFunctionFirstArg(const std::string & name, const DataTypes & arguments, const Array & params, EarlyWindowFunctionInfo & early_window_info)
{
    if (arguments.size() != 3)
        throw Exception("Early window function " + name + " requires one timestamp argument and one value.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    early_window_info.keys.push_back(2);

    WhichDataType which(*arguments[0]);
    if (which.isUInt8()) return createEarlyWindowFunctionSecondArg<GeneralData, StringData, UInt8>(name, arguments, params);
    if (which.isUInt16()) return createEarlyWindowFunctionSecondArg<GeneralData, StringData, UInt16>(name, arguments, params);
    if (which.isUInt32()) return createEarlyWindowFunctionSecondArg<GeneralData, StringData, UInt32>(name, arguments, params);
    if (which.isUInt64()) return createEarlyWindowFunctionSecondArg<GeneralData, StringData, UInt64>(name, arguments, params);
    if (which.isDate()) return createEarlyWindowFunctionSecondArg<GeneralData, StringData, DataTypeDate::FieldType>(name, arguments, params);
    if (which.isDateTime()) return createEarlyWindowFunctionSecondArg<GeneralData, StringData, DataTypeDateTime::FieldType>(name, arguments, params);

    throw Exception{"Illegal type " + arguments.front().get()->getName()
            + " of first argument of aggregate function " + name + ", must be Unsigned Number, Date, DateTime",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

}

void registerEarlyWindowFunction(EarlyWindowFunctionFactory & factory)
{
    factory.registerFunction("windowLast", createEarlyWindowFunctionFirstArg<EarlyWindowFunctionLastGeneralData, EarlyWindowFunctionLastStringData>, EarlyWindowFunctionFactory::CaseInsensitive);
    factory.registerFunction("windowFirst", createEarlyWindowFunctionFirstArg<EarlyWindowFunctionFirstGeneralData, EarlyWindowFunctionFirstStringData>, EarlyWindowFunctionFactory::CaseInsensitive);
}

}
