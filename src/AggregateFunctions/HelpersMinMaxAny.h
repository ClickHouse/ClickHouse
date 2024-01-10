#pragma once

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/SingleValueData.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// min, max, any, anyLast, anyHeavy, etc...
template <template <typename> class AggregateFunctionTemplate>
static IAggregateFunction *
createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<SingleValueDataFixed<TYPE>>(argument_type); /// NOLINT
    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDate::FieldType>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDateTime::FieldType>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<SingleValueDataString>(argument_type);

    return new AggregateFunctionTemplate<SingleValueDataGeneric>(argument_type);
}

/// Functions that inherit from SingleValueData*
/// singleValueOrNull
template <template <typename> class AggregateFunctionTemplate, template <typename> class ChildType>
static IAggregateFunction * createAggregateFunctionSingleValueComposite(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<ChildType<SingleValueDataFixed<TYPE>>>(argument_type); /// NOLINT
    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<ChildType<SingleValueDataFixed<DataTypeDate::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<ChildType<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<ChildType<SingleValueDataString>>(argument_type);

    return new AggregateFunctionTemplate<ChildType<SingleValueDataGeneric>>(argument_type);
}


/// For possible values for template parameters, see 'AggregateFunctionMinMaxAny.h'.
struct AggregateFunctionArgMinMaxData
{
private:
    char r_data[SingleValueDataBase::MAX_STORAGE_SIZE];
    char v_data[SingleValueDataBase::MAX_STORAGE_SIZE];

    static void generateSingleValueFromTypeIndex(TypeIndex idx, char data[SingleValueDataBase::MAX_STORAGE_SIZE])
    {
#define DISPATCH(TYPE) \
    if (idx == TypeIndex::TYPE) \
    { \
        static_assert(sizeof(SingleValueDataFixed<TYPE>) <= SingleValueDataBase::MAX_STORAGE_SIZE); \
        new (data) SingleValueDataFixed<TYPE>(); \
        return; \
    }

        FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

        if (idx == TypeIndex::Date)
        {
            static_assert(sizeof(SingleValueDataFixed<DataTypeDate::FieldType>) <= SingleValueDataBase::MAX_STORAGE_SIZE);
            new (data) SingleValueDataFixed<DataTypeDate::FieldType>;
            return;
        }
        if (idx == TypeIndex::DateTime)
        {
            static_assert(sizeof(SingleValueDataFixed<DataTypeDateTime::FieldType>) <= SingleValueDataBase::MAX_STORAGE_SIZE);
            new (data) SingleValueDataFixed<DataTypeDateTime::FieldType>;
            return;
        }
        if (idx == TypeIndex::String)
        {
            static_assert(sizeof(SingleValueDataString) <= SingleValueDataBase::MAX_STORAGE_SIZE);
            new (data) SingleValueDataString;
            return;
        }
        static_assert(sizeof(SingleValueDataGeneric) <= SingleValueDataBase::MAX_STORAGE_SIZE);
        new (data) SingleValueDataGeneric;
    }

public:
    SingleValueDataBase & result() { return *reinterpret_cast<SingleValueDataBase *>(r_data); }

    SingleValueDataBase const & result() const { return *reinterpret_cast<const SingleValueDataBase *>(r_data); }

    SingleValueDataBase & value() { return *reinterpret_cast<SingleValueDataBase *>(v_data); }

    SingleValueDataBase const & value() const { return *reinterpret_cast<const SingleValueDataBase *>(v_data); }

    [[noreturn]] explicit AggregateFunctionArgMinMaxData()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionArgMinMaxData initialized empty");
    }

    explicit AggregateFunctionArgMinMaxData(TypeIndex result_type, TypeIndex value_type)
    {
        generateSingleValueFromTypeIndex(result_type, r_data);
        generateSingleValueFromTypeIndex(value_type, v_data);
    }
};

static_assert(
    sizeof(AggregateFunctionArgMinMaxData) == 2 * SingleValueDataBase::MAX_STORAGE_SIZE,
    "Incorrect size of AggregateFunctionArgMinMaxData struct");


template <template <typename> class AggregateFunctionTemplate>
static IAggregateFunction *
createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];

    return new AggregateFunctionTemplate<AggregateFunctionArgMinMaxData>(res_type, val_type);
}
}
