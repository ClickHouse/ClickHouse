#pragma once

#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>


#define FOR_UNSIGNED_INTEGER_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64)

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

#define FOR_NUMERIC_TYPES_AND_ENUMS(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64) \
    M(UInt8) \
    M(UInt16)

namespace DB
{

/** Create an aggregate function with a numeric type in the template parameter, depending on the type of the argument.
  */
template <template <typename, typename ... TArgs> class AggregateFunctionTemplate, typename ... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(FIELDTYPE) \
    if (which.idx == TypeIndex::FIELDTYPE) return new AggregateFunctionTemplate<FIELDTYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename ... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(FIELDTYPE) \
    if (which.idx == TypeIndex::FIELDTYPE) return new AggregateFunctionTemplate<FIELDTYPE, Data>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename ... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(FIELDTYPE) \
    if (which.idx == TypeIndex::FIELDTYPE) return new AggregateFunctionTemplate<FIELDTYPE, Data<FIELDTYPE>>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}


template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data, typename ... TArgs>
static IAggregateFunction * createWithUnsignedIntegerType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
#define DISPATCH(FIELDTYPE) \
    if (which.idx == TypeIndex::FIELDTYPE) return new AggregateFunctionTemplate<FIELDTYPE, Data<FIELDTYPE>>(std::forward<TArgs>(args)...);
    FOR_UNSIGNED_INTEGER_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}


/** For template with two arguments.
  */
template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename ... TArgs>
static IAggregateFunction * createWithTwoNumericTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);
#define DISPATCH(FIELDTYPE) \
    if (which.idx == TypeIndex::FIELDTYPE) return new AggregateFunctionTemplate<FirstType, FIELDTYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename ... TArgs>
static IAggregateFunction * createWithTwoNumericTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);
#define DISPATCH(FIELDTYPE) \
    if (which.idx == TypeIndex::FIELDTYPE) \
        return createWithTwoNumericTypesSecond<FIELDTYPE, AggregateFunctionTemplate>(second_type, std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

}
