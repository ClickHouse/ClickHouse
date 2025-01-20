#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <AggregateFunctions/AggregateFunctionGroupNumericIndexedVector.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    template <typename FirstType, template <typename, typename> class VectorImpl, typename... TArgs>
    IAggregateFunction *
    createVectorWithTwoNumericTypesSecond(const IDataType & second_type, const DataTypes & types, const Array & params, TArgs &&... args)
    {
        WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionNumericIndexedVector<FirstType, TYPE, VectorImpl, std::decay_t<TArgs>...>( \
            types, params, std::forward<TArgs>(args)...);
        FOR_NUMERIC_INDEXED_VECTOR_VALUE_TYPES(DISPATCH)
#undef DISPATCH
        return nullptr;
    }

    template <template <typename, typename> class VectorImpl, typename... TArgs>
    IAggregateFunction * createVectorWithTwoNumericTypesFirst(
        const IDataType & first_type, const IDataType & second_type, const DataTypes & types, const Array & params, TArgs &&... args)
    {
        WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createVectorWithTwoNumericTypesSecond<TYPE, VectorImpl>(second_type, types, params, std::forward<TArgs>(args)...);
        FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(DISPATCH)
#undef DISPATCH
        return nullptr;
    }

    AggregateFunctionPtr createAggregateFunctionNumericIndexedVector(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertBinary(name, argument_types);

        String numeric_index_vector_type = "BSI";
        UInt32 integer_bit_num = BSINumericIndexedVector<UInt32, Float64>::DEFAULT_INTEGER_BIT_NUM;
        UInt32 fraction_bit_num = BSINumericIndexedVector<UInt32, Float64>::DEFAULT_FRACTION_BIT_NUM;

        if (!parameters.empty())
        {
            if (parameters.size() != 3)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "AggregateFunction {} requires zero/three parameters", name);
            numeric_index_vector_type = applyVisitor(FieldVisitorToString(), parameters[0]);
            if (numeric_index_vector_type != "BSI")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "First parameter of AggregateFunction {} must be 'BSI'", name);
            integer_bit_num = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[1]);
            if (integer_bit_num > BSINumericIndexedVector<UInt32, Float64>::MAX_INTEGER_BIT_NUM)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "integer_bit_num must be less than or equal {}",
                    BSINumericIndexedVector<UInt32, Float64>::MAX_INTEGER_BIT_NUM);
            fraction_bit_num = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[2]);
            if (fraction_bit_num > BSINumericIndexedVector<UInt32, Float64>::MAX_FRACTION_BIT_NUM)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "fraction_bit_num must be less than or equal {}",
                    BSINumericIndexedVector<UInt32, Float64>::MAX_FRACTION_BIT_NUM);
        }

        if (!argument_types[0]->canBeUsedInBitOperations())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The type {} of argument for aggregate function {} is illegal, because it cannot be used in Bitmap operations",
                    argument_types[0]->getName(), name);

        WhichDataType which(argument_types[1]->getTypeId());
        if (!which.isNativeUInt() && !which.isNativeInt() && !which.isFloat())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument type must be NativeUInt or NativeInt or Float");

        AggregateFunctionPtr res = nullptr;
        if (numeric_index_vector_type == "BSI")
        {
            res = std::shared_ptr<const IAggregateFunction>(createVectorWithTwoNumericTypesFirst<BSINumericIndexedVector>(
                *argument_types[0], *argument_types[1], argument_types, parameters, integer_bit_num, fraction_bit_num));
        }

        if (!res)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal types {} and {} of arguments for aggregate function {}",
                argument_types[0]->getName(), argument_types[1]->getName(), name);
        return res;
    }
}

void registerAggregateFunctionsNumericIndexedVector(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupNumericIndexedVector", createAggregateFunctionNumericIndexedVector);
}


}
