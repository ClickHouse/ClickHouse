#include <AggregateFunctions/AggregateFunctionSparkbar.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename ... TArgs>
IAggregateFunction * createWithUIntegerOrTimeType(const std::string & name, const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date || which.idx == TypeIndex::UInt16) return new AggregateFunctionTemplate<UInt16, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<UInt32, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt8) return new AggregateFunctionTemplate<UInt8, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<UInt64, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt128) return new AggregateFunctionTemplate<UInt128, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt256) return new AggregateFunctionTemplate<UInt256, Data>(std::forward<TArgs>(args)...);
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first argument type must be UInt or Date or DateTime for aggregate function {}", name);
}

template <typename ... TArgs>
AggregateFunctionPtr createAggregateFunctionSparkbarImpl(const std::string & name, const IDataType & x_argument_type, const IDataType & y_argument_type, TArgs ... args)
{
    WhichDataType which(y_argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return AggregateFunctionPtr(createWithUIntegerOrTimeType<AggregateFunctionSparkbar, TYPE>(name, x_argument_type, std::forward<TArgs>(args)...));
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument type must be numeric for aggregate function {}", name);
}


AggregateFunctionPtr createAggregateFunctionSparkbar(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    assertBinary(name, arguments);

    if (params.size() != 1 && params.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The number of params does not match for aggregate function '{}', expected 1 or 3, got {}", name, params.size());

    if (params.size() == 3)
    {
        if (params.at(1).getType() != arguments[0]->getDefault().getType() ||
            params.at(2).getType() != arguments[0]->getDefault().getType())
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The second and third parameters are not the same type as the first arguments for aggregate function {}", name);
        }
    }
    return createAggregateFunctionSparkbarImpl(name, *arguments[0], *arguments[1], arguments, params);
}

}

void registerAggregateFunctionSparkbar(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sparkbar", createAggregateFunctionSparkbar);
}

}
