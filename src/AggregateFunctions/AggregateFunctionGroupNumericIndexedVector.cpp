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
createBSIVectorWithTwoNumericTypesSecond(const IDataType & second_type, const DataTypes & types, const Array & params, TArgs &&... args)
{
    WhichDataType which(second_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionNumericIndexedVector<VectorImpl<FirstType, TYPE>, std::decay_t<TArgs>...>( \
            types, params, std::forward<TArgs>(args)...);
    FOR_NUMERIC_INDEXED_VECTOR_VALUE_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class VectorImpl, typename... TArgs>
IAggregateFunction * createBSIVectorWithTwoNumericTypesFirst(
    const IDataType & first_type, const IDataType & second_type, const DataTypes & types, const Array & params, TArgs &&... args)
{
    WhichDataType which(first_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createBSIVectorWithTwoNumericTypesSecond<TYPE, VectorImpl>(second_type, types, params, std::forward<TArgs>(args)...);
    FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

AggregateFunctionPtr createAggregateFunctionNumericIndexedVector(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    AggregateFunctionPtr res = nullptr;

    WhichDataType first_which(argument_types[0]->getTypeId());
    WhichDataType second_which(argument_types[1]->getTypeId());

    if (parameters.empty() || applyVisitor(FieldVisitorToString(), parameters[0]) == "BSI")
    {
        String numeric_index_vector_type = "BSI";

        UInt32 integer_bit_num;
        UInt32 fraction_bit_num;

        if (!parameters.empty() && parameters.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "AggregateFunction {} requires zero/three parameters", name);

        if (parameters.size() == 3)
        {
            integer_bit_num = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[1]);
            fraction_bit_num = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[2]);
        }
        else
        {
            if (second_which.isUInt() or second_which.isInt())
            {
                switch (second_which.idx)
                {
                    case TypeIndex::UInt8:
                        integer_bit_num = 8;
                        break;
                    case TypeIndex::UInt16:
                        integer_bit_num = 16;
                        break;
                    case TypeIndex::UInt32:
                        integer_bit_num = 32;
                        break;
                    case TypeIndex::UInt64:
                        integer_bit_num = 64;
                        break;
                    case TypeIndex::Int8:
                        integer_bit_num = 8;
                        break;
                    case TypeIndex::Int16:
                        integer_bit_num = 16;
                        break;
                    case TypeIndex::Int32:
                        integer_bit_num = 32;
                        break;
                    case TypeIndex::Int64:
                        integer_bit_num = 64;
                        break;
                    default:
                        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for {} must be NativeInt/NativeUInt.", name);
                }
                fraction_bit_num = 0;
            }
            else
            {
                integer_bit_num = BSINumericIndexedVector<UInt32, Float64>::DEFAULT_INTEGER_BIT_NUM;
                fraction_bit_num = BSINumericIndexedVector<UInt32, Float64>::DEFAULT_FRACTION_BIT_NUM;
            }
        }

        if (!argument_types[0]->canBeUsedInBitOperations())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The type {} of argument for aggregate function {} is illegal, because it cannot be used in Bitmap operations",
                argument_types[0]->getName(),
                name);

        if (first_which.isUInt64() or first_which.isInt64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument type doesn't support UInt64/Int64 in BSI");

        if (!second_which.isNativeUInt() && !second_which.isNativeInt() && !second_which.isFloat())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument type must be NativeUInt or NativeInt or Float");

        res = std::shared_ptr<const IAggregateFunction>(createBSIVectorWithTwoNumericTypesFirst<BSINumericIndexedVector>(
            *argument_types[0], *argument_types[1], argument_types, parameters, integer_bit_num, fraction_bit_num));
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupport first parameter of AggregateFunction {}", name);
    }

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types {} and {} of arguments for aggregate function {}",
            argument_types[0]->getName(),
            argument_types[1]->getName(),
            name);
    return res;
}
}

void registerAggregateFunctionsNumericIndexedVector(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupNumericIndexedVector", createAggregateFunctionNumericIndexedVector);
}


}
