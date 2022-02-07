#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupSortedArray.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Common/FieldVisitorConvertToNumber.h>


static inline constexpr UInt64 GROUP_SORTED_ARRAY_MAX_SIZE = 0xFFFFFF;
static inline constexpr UInt64 GROUP_SORTED_ARRAY_DEFAULT_THRESHOLD = 10;


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{
    template <typename T, bool expr_sorted, typename TColumnB, bool is_plain_b>
    class AggregateFunctionGroupSortedArrayNumeric : public AggregateFunctionGroupSortedArray<T, false, expr_sorted, TColumnB, is_plain_b>
    {
        using AggregateFunctionGroupSortedArray<T, false, expr_sorted, TColumnB, is_plain_b>::AggregateFunctionGroupSortedArray;
    };

    template <typename T, bool expr_sorted, typename TColumnB, bool is_plain_b>
    class AggregateFunctionGroupSortedArrayFieldType
        : public AggregateFunctionGroupSortedArray<typename T::FieldType, false, expr_sorted, TColumnB, is_plain_b>
    {
        using AggregateFunctionGroupSortedArray<typename T::FieldType, false, expr_sorted, TColumnB, is_plain_b>::
            AggregateFunctionGroupSortedArray;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<T>()); }
    };

    template <bool expr_sorted = false, typename TColumnB = UInt64, bool is_plain_b = false>
    AggregateFunctionPtr
    createAggregateFunctionGroupSortedArrayTyped(const DataTypes & argument_types, const Array & params, UInt64 threshold)
    {
#define DISPATCH(A, CLS, B) \
    if (which.idx == TypeIndex::A) \
        return AggregateFunctionPtr(new CLS<B, expr_sorted, TColumnB, is_plain_b>(threshold, argument_types, params));
#define DISPATCH_NUMERIC(A) DISPATCH(A, AggregateFunctionGroupSortedArrayNumeric, A)
        WhichDataType which(argument_types[0]);
        FOR_NUMERIC_TYPES(DISPATCH_NUMERIC)
        DISPATCH(Enum8, AggregateFunctionGroupSortedArrayNumeric, Int8)
        DISPATCH(Enum16, AggregateFunctionGroupSortedArrayNumeric, Int16)
        DISPATCH(Date, AggregateFunctionGroupSortedArrayFieldType, DataTypeDate)
        DISPATCH(DateTime, AggregateFunctionGroupSortedArrayFieldType, DataTypeDateTime)
#undef DISPATCH
#undef DISPATCH_NUMERIC

        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            return AggregateFunctionPtr(new AggregateFunctionGroupSortedArray<StringRef, true, expr_sorted, TColumnB, is_plain_b>(
                threshold, argument_types, params));
        }
        else
        {
            return AggregateFunctionPtr(new AggregateFunctionGroupSortedArray<StringRef, false, expr_sorted, TColumnB, is_plain_b>(
                threshold, argument_types, params));
        }
    }


    AggregateFunctionPtr createAggregateFunctionGroupSortedArray(
        const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        UInt64 threshold = GROUP_SORTED_ARRAY_DEFAULT_THRESHOLD;

        if (params.size() == 1)
        {
            UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

            if (k > GROUP_SORTED_ARRAY_MAX_SIZE)
                throw Exception(
                    "Too large parameter(s) for aggregate function " + name + ". Maximum: " + toString(GROUP_SORTED_ARRAY_MAX_SIZE),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            if (k == 0)
                throw Exception("Parameter 0 is illegal for aggregate function " + name, ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            threshold = k;
        }
        else if (params.size() != 0)
        {
            throw Exception("Aggregate function " + name + " only supports 1 parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (argument_types.size() == 2)
        {
            if (isNumber(argument_types[1]))
            {
#define DISPATCH2(A, B) \
    if (which.idx == TypeIndex::A) \
        return createAggregateFunctionGroupSortedArrayTyped<true, B>(argument_types, params, threshold);
#define DISPATCH(A) DISPATCH2(A, A)
                WhichDataType which(argument_types[1]);
                FOR_NUMERIC_TYPES(DISPATCH)
                DISPATCH2(Enum8, Int8)
                DISPATCH2(Enum16, Int16)
#undef DISPATCH
#undef DISPATCH2
                throw Exception("Invalid parameter type.", ErrorCodes::BAD_ARGUMENTS);
            }
            else if (argument_types[1]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            {
                return createAggregateFunctionGroupSortedArrayTyped<true, StringRef, true>(argument_types, params, threshold);
            }
            else
            {
                return createAggregateFunctionGroupSortedArrayTyped<true, StringRef, false>(argument_types, params, threshold);
            }
        }
        else if (argument_types.size() == 1)
        {
            return createAggregateFunctionGroupSortedArrayTyped<>(argument_types, params, threshold);
        }
        else
        {
            throw Exception(
                "Aggregate function " + name + " requires one or two parameters.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }
}

void registerAggregateFunctionGroupSortedArray(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("groupSortedArray", {createAggregateFunctionGroupSortedArray, properties});
}
}
