#pragma once

#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>

#define FOR_BASIC_NUMERIC_TYPES(M) \
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

#define FOR_NUMERIC_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) \
    M(UInt256) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Int128) \
    M(Int256) \
    M(Float32) \
    M(Float64)

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
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<Data<TYPE>>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Data<Int8>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Data<Int16>>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, bool> class AggregateFunctionTemplate, bool bool_param, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, bool_param>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, bool_param>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, bool_param>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, Data>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, Data>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, Data<TYPE>>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, Data<Int8>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, Data<Int16>>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithUnsignedIntegerType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::UInt8) return new AggregateFunctionTemplate<UInt8, Data<UInt8>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt16) return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<UInt64, Data<UInt64>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt128) return new AggregateFunctionTemplate<UInt128, Data<UInt128>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt256) return new AggregateFunctionTemplate<UInt256, Data<UInt256>>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename... TArgs>
static IAggregateFunction * createWithBasicNumberOrDateOrDateTime(const IDataType & argument_type, TArgs &&... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<TYPE, Data<TYPE>>(std::forward<TArgs>(args)...);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(std::forward<TArgs>(args)...);

    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithNumericBasedType(const IDataType & argument_type, TArgs && ... args)
{
    IAggregateFunction * f = createWithNumericType<AggregateFunctionTemplate>(argument_type, std::forward<TArgs>(args)...);
    if (f)
        return f;

    /// expects that DataTypeDate based on UInt16, DataTypeDateTime based on UInt32
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<UInt16>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<UInt32>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UUID) return new AggregateFunctionTemplate<UUID>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithDecimalType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) return new AggregateFunctionTemplate<Decimal32>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal64) return new AggregateFunctionTemplate<Decimal64>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal128) return new AggregateFunctionTemplate<Decimal128>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal256) return new AggregateFunctionTemplate<Decimal256>(std::forward<TArgs>(args)...);
    if constexpr (AggregateFunctionTemplate<DateTime64>::DateTime64Supported)
        if (which.idx == TypeIndex::DateTime64) return new AggregateFunctionTemplate<DateTime64>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename... TArgs>
static IAggregateFunction * createWithDecimalType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) return new AggregateFunctionTemplate<Decimal32, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal64) return new AggregateFunctionTemplate<Decimal64, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal128) return new AggregateFunctionTemplate<Decimal128, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Decimal256) return new AggregateFunctionTemplate<Decimal256, Data>(std::forward<TArgs>(args)...);
    if constexpr (AggregateFunctionTemplate<DateTime64, Data>::DateTime64Supported)
        if (which.idx == TypeIndex::DateTime64) return new AggregateFunctionTemplate<DateTime64, Data>(std::forward<TArgs>(args)...);
    return nullptr;
}

/** For template with two arguments.
  */
template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<FirstType, TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<FirstType, Int8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<FirstType, Int16>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createWithTwoNumericTypesSecond<TYPE, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8)
        return createWithTwoNumericTypesSecond<Int8, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16)
        return createWithTwoNumericTypesSecond<Int16, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    return nullptr;
}

template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoBasicNumericTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<FirstType, TYPE>(std::forward<TArgs>(args)...);
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
        return createWithTwoBasicNumericTypesSecond<TYPE, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericOrDateTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<FirstType, TYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<FirstType, Int8>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<FirstType, Int16>(std::forward<TArgs>(args)...);

    /// expects that DataTypeDate based on UInt16, DataTypeDateTime based on UInt32
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<FirstType, UInt16>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<FirstType, UInt32>(std::forward<TArgs>(args)...);

    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericOrDateTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createWithTwoNumericOrDateTypesSecond<TYPE, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Enum8)
        return createWithTwoNumericOrDateTypesSecond<Int8, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Enum16)
        return createWithTwoNumericOrDateTypesSecond<Int16, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);

    /// expects that DataTypeDate based on UInt16, DataTypeDateTime based on UInt32
    if (which.idx == TypeIndex::Date)
        return createWithTwoNumericOrDateTypesSecond<UInt16, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime)
        return createWithTwoNumericOrDateTypesSecond<UInt32, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithStringType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::String) return new AggregateFunctionTemplate<String>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::FixedString) return new AggregateFunctionTemplate<String>(std::forward<TArgs>(args)...);
    return nullptr;
}

}
