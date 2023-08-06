//
// Created by sinsinan on 04/08/23.
//

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionLTTB.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB
{
struct Settings;

namespace
{

    AggregateFunctionPtr
    createAggregateFunctionLTTB(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertBinary(name, argument_types);

        WhichDataType whichColumnX(argument_types[0]);

        if (!(whichColumnX.idx == TypeIndex::DateTime64 || whichColumnX.idx == TypeIndex::DateTime || isNumber(argument_types[0])))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports DateTime, DateTime64 or Number as arg 1", name);

        if (!isNumber(argument_types[1]))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports Numbers as arg 2", name);

        int scale;
        if (const auto * datetime64_type = typeid_cast<const DataTypeDateTime64 *>(argument_types[0].get()))
        {
            scale = datetime64_type->getScale();
        } else
        {
            scale = 0;
        }
        return std::make_shared<AggregateFunctionLTTB>(argument_types, parameters, scale, whichColumnX.idx);
    }

}


void registerAggregateFunctionLTTB(AggregateFunctionFactory & factory)
{
    factory.registerFunction("lttb", createAggregateFunctionLTTB);
}


}
