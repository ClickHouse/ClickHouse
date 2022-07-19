#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <AggregateFunctions/AggregateFunctionGroupBitmap.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    template <template <typename, typename> class AggregateFunctionTemplate, template <typename> typename Data, typename... TArgs>
    IAggregateFunction * createWithIntegerType(const IDataType & argument_type, TArgs &&... args)
    {
        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::UInt8) return new AggregateFunctionTemplate<UInt8, Data<UInt8>>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::UInt16) return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<UInt64, Data<UInt64>>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::Int8) return new AggregateFunctionTemplate<Int8, Data<Int8>>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::Int16) return new AggregateFunctionTemplate<Int16, Data<Int16>>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::Int32) return new AggregateFunctionTemplate<Int32, Data<Int32>>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::Int64) return new AggregateFunctionTemplate<Int64, Data<Int64>>(std::forward<TArgs>(args)...);
        return nullptr;
    }

    template <template <typename> typename Data>
    AggregateFunctionPtr createAggregateFunctionBitmap(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);

        if (!argument_types[0]->canBeUsedInBitOperations())
            throw Exception(
                "The type " + argument_types[0]->getName() + " of argument for aggregate function " + name
                    + " is illegal, because it cannot be used in Bitmap operations",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        AggregateFunctionPtr res(createWithIntegerType<AggregateFunctionBitmap, Data>(*argument_types[0], argument_types[0]));

        if (!res)
            throw Exception(
                "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }

    // Additional aggregate functions to manipulate bitmaps.
    template <template <typename, typename> typename AggregateFunctionTemplate>
    AggregateFunctionPtr createAggregateFunctionBitmapL2(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);

        DataTypePtr argument_type_ptr = argument_types[0];
        WhichDataType which(*argument_type_ptr);
        if (which.idx != TypeIndex::AggregateFunction)
            throw Exception(
                "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        /// groupBitmap needs to know about the data type that was used to create bitmaps.
        /// We need to look inside the type of its argument to obtain it.
        const DataTypeAggregateFunction & datatype_aggfunc = dynamic_cast<const DataTypeAggregateFunction &>(*argument_type_ptr);
        AggregateFunctionPtr aggfunc = datatype_aggfunc.getFunction();
        DataTypePtr nested_argument_type_ptr = aggfunc->getArgumentTypes()[0];

        AggregateFunctionPtr res(createWithIntegerType<AggregateFunctionTemplate, AggregateFunctionGroupBitmapData>(
            *nested_argument_type_ptr, argument_type_ptr));

        if (!res)
            throw Exception(
                "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }
}

void registerAggregateFunctionsBitmap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupBitmap", createAggregateFunctionBitmap<AggregateFunctionGroupBitmapData>);
    factory.registerFunction("groupBitmapAnd", createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2And>);
    factory.registerFunction("groupBitmapOr", createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2Or>);
    factory.registerFunction("groupBitmapXor", createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2Xor>);
}

}
