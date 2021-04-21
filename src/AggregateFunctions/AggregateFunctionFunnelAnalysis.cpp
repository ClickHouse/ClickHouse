#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFunnelAnalysis.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/typeid_cast.h>

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

template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionFunnelAnalysis(const std::string & name, const DataTypes & arguments, const Array & params)
{
    if (params.size() < 3)
        throw Exception{"Aggregate function " + name + " requires at least three parameters: <window>, <strict>, <strict_order>, [event1, [event2, ...]]", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    if (arguments.size() < 2)
        throw Exception("Aggregate function " + name + " requires one event argument and one timestamps argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (params.size() > max_events + 3)
        throw Exception("Too many event parameters for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * ary_data_type_ptr = typeid_cast<const DataTypeArray *>(arguments[1].get());
    if (!ary_data_type_ptr)
        throw Exception("Data type of column timestamps (" + arguments[1]->getName() + ") is not of array type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    DataTypePtr ts_data_type_ptr = ary_data_type_ptr->getNestedType();
    AggregateFunctionPtr res(createWithUnsignedIntegerType<AggregateFunctionFunnelAnalysis, Data>(*ts_data_type_ptr, arguments, params));
    WhichDataType which(*ts_data_type_ptr);
    if (res)
        return res;
    else if (which.isDate())
        return std::make_shared<AggregateFunctionFunnelAnalysis<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>>>(arguments, params);
    else if (which.isDateTime())
        return std::make_shared<AggregateFunctionFunnelAnalysis<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>>>(arguments, params);

    throw Exception{"Illegal element type " + ts_data_type_ptr->getName()
            + " of second argument of aggregate function " + name + ", must be Unsigned Number, Date, DateTime",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

}

void registerAggregateFunctionFunnelAnalysis(AggregateFunctionFactory & factory)
{
    factory.registerFunction("funnelAnalysis", createAggregateFunctionFunnelAnalysis<AggregateFunctionFunnelAnalysisData>, AggregateFunctionFactory::CaseInsensitive);
}

}
