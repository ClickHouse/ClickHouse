#pragma once

#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{
struct Settings;

/** Create an aggregate function with a numeric type in the template parameter, depending on the type of the argument.
  */
template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE>(args...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16>(args...);
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<Data<TYPE>>(args...); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Data<Int8>>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Data<Int16>>(args...);
    return nullptr;
}

template <template <typename, bool> class AggregateFunctionTemplate, bool bool_param, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, bool_param>(args...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, bool_param>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, bool_param>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, Data>(args...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, Data>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, Data>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, Data<TYPE>>(args...); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, Data<Int8>>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, Data<Int16>>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename, bool> class Data, bool bool_param, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, Data<TYPE, bool_param>>(args...); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, Data<Int8, bool_param>>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, Data<Int16, bool_param>>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithUnsignedIntegerType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::UInt8) return new AggregateFunctionTemplate<UInt8, Data<UInt8>>(args...);
    if (which.idx == TypeIndex::UInt16) return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(args...);
    if (which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(args...);
    if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<UInt64, Data<UInt64>>(args...);
    if (which.idx == TypeIndex::UInt128) return new AggregateFunctionTemplate<UInt128, Data<UInt128>>(args...);
    if (which.idx == TypeIndex::UInt256) return new AggregateFunctionTemplate<UInt256, Data<UInt256>>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithSignedIntegerType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Int8) return new AggregateFunctionTemplate<Int8, Data<Int8>>(args...);
    if (which.idx == TypeIndex::Int16) return new AggregateFunctionTemplate<Int16, Data<Int16>>(args...);
    if (which.idx == TypeIndex::Int32) return new AggregateFunctionTemplate<Int32, Data<Int32>>(args...);
    if (which.idx == TypeIndex::Int64) return new AggregateFunctionTemplate<Int64, Data<Int64>>(args...);
    if (which.idx == TypeIndex::Int128) return new AggregateFunctionTemplate<Int128, Data<Int128>>(args...);
    if (which.idx == TypeIndex::Int256) return new AggregateFunctionTemplate<Int256, Data<Int256>>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithIntegerType(const IDataType & argument_type, TArgs && ... args)
{
    IAggregateFunction * f = createWithUnsignedIntegerType<AggregateFunctionTemplate, Data>(argument_type, args...);
    if (f)
        return f;
    return createWithSignedIntegerType<AggregateFunctionTemplate, Data>(argument_type, args...);
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithBasicNumberOrDateOrDateTime(const IDataType & argument_type, TArgs &&... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE, Data<TYPE>>(args...); /// NOLINT
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(args...);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(args...);

    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithNumericBasedType(const IDataType & argument_type, TArgs && ... args)
{
    IAggregateFunction * f = createWithNumericType<AggregateFunctionTemplate>(argument_type, args...);
    if (f)
        return f;

    /// expects that DataTypeDate based on UInt16, DataTypeDateTime based on UInt32
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<UInt16>(args...);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<UInt32>(args...);
    if (which.idx == TypeIndex::UUID) return new AggregateFunctionTemplate<UUID>(args...);
    if (which.idx == TypeIndex::IPv4) return new AggregateFunctionTemplate<IPv4>(args...);
    if (which.idx == TypeIndex::IPv6) return new AggregateFunctionTemplate<IPv6>(args...);
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithDecimalType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) return new AggregateFunctionTemplate<Decimal32>(args...);
    if (which.idx == TypeIndex::Decimal64) return new AggregateFunctionTemplate<Decimal64>(args...);
    if (which.idx == TypeIndex::Decimal128) return new AggregateFunctionTemplate<Decimal128>(args...);
    if (which.idx == TypeIndex::Decimal256) return new AggregateFunctionTemplate<Decimal256>(args...);
    if constexpr (AggregateFunctionTemplate<DateTime64>::DateTime64Supported)
        if (which.idx == TypeIndex::DateTime64) return new AggregateFunctionTemplate<DateTime64>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename... TArgs>
static IAggregateFunction * createWithDecimalType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) return new AggregateFunctionTemplate<Decimal32, Data>(args...);
    if (which.idx == TypeIndex::Decimal64) return new AggregateFunctionTemplate<Decimal64, Data>(args...);
    if (which.idx == TypeIndex::Decimal128) return new AggregateFunctionTemplate<Decimal128, Data>(args...);
    if (which.idx == TypeIndex::Decimal256) return new AggregateFunctionTemplate<Decimal256, Data>(args...);
    if constexpr (AggregateFunctionTemplate<DateTime64, Data>::DateTime64Supported)
        if (which.idx == TypeIndex::DateTime64) return new AggregateFunctionTemplate<DateTime64, Data>(args...);
    return nullptr;
}

/** For template with two arguments.
  */
template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<FirstType, TYPE>(args...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<FirstType, Int8>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<FirstType, Int16>(args...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createWithTwoNumericTypesSecond<TYPE, AggregateFunctionTemplate>(second_type, args...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8)
        return createWithTwoNumericTypesSecond<Int8, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Enum16)
        return createWithTwoNumericTypesSecond<Int16, AggregateFunctionTemplate>(second_type, args...);
    return nullptr;
}

template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoBasicNumericTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<FirstType, TYPE>(args...);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoBasicNumericTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createWithTwoBasicNumericTypesSecond<TYPE, AggregateFunctionTemplate>(second_type, args...);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericOrDateTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<FirstType, TYPE>(args...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<FirstType, Int8>(args...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<FirstType, Int16>(args...);

    /// expects that DataTypeDate based on UInt16, DataTypeDateTime based on UInt32
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<FirstType, UInt16>(args...);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<FirstType, UInt32>(args...);

    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericOrDateTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createWithTwoNumericOrDateTypesSecond<TYPE, AggregateFunctionTemplate>(second_type, args...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8)
        return createWithTwoNumericOrDateTypesSecond<Int8, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Enum16)
        return createWithTwoNumericOrDateTypesSecond<Int16, AggregateFunctionTemplate>(second_type, args...);

    /// expects that DataTypeDate based on UInt16, DataTypeDateTime based on UInt32
    if (which.idx == TypeIndex::Date)
        return createWithTwoNumericOrDateTypesSecond<UInt16, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::DateTime)
        return createWithTwoNumericOrDateTypesSecond<UInt32, AggregateFunctionTemplate>(second_type, args...);
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithStringType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::String) return new AggregateFunctionTemplate<String>(args...);
    if (which.idx == TypeIndex::FixedString) return new AggregateFunctionTemplate<String>(args...);
    return nullptr;
}

}
