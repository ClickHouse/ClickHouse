#pragma once

#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>

#define FOR_NUMERIC_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64)

namespace DB
{

/** Create an aggregate function with a numeric type in the template parameter, depending on the type of the argument.
  */
template <template <typename> class AggregateFunctionTemplate, typename ... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<UInt8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<UInt16>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename ... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, Data>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<UInt8, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<UInt16, Data>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename ... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, Data<TYPE>>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<UInt8, Data<UInt8>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename ... TArgs>
static IAggregateFunction * createWithUnsignedIntegerType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::UInt8) return new AggregateFunctionTemplate<UInt8, Data<UInt8>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt16) return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<UInt64, Data<UInt64>>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename ... TArgs>
static IAggregateFunction * createWithDecimalType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) return new AggregateFunctionTemplate<Decimal32>(argument_type, std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal64) return new AggregateFunctionTemplate<Decimal64>(argument_type, std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal128) return new AggregateFunctionTemplate<Decimal128>(argument_type, std::forward<TArgs>(args)...);
    return nullptr;
}


/** For template with two arguments.
  */
template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename ... TArgs>
static IAggregateFunction * createWithTwoNumericTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<FirstType, TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<FirstType, UInt8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<FirstType, UInt16>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename ... TArgs>
static IAggregateFunction * createWithTwoNumericTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createWithTwoNumericTypesSecond<TYPE, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8)
        return createWithTwoNumericTypesSecond<UInt8, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16)
        return createWithTwoNumericTypesSecond<UInt16, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    return nullptr;
}

}
