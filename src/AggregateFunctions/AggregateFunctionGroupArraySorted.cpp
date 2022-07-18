#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArraySorted.h>
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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{
    template <typename T, bool expr_sorted, typename TColumnB, bool is_plain_b>
    class AggregateFunctionGroupArraySortedNumeric : public AggregateFunctionGroupArraySorted<T, false, expr_sorted, TColumnB, is_plain_b>
    {
        using AggregateFunctionGroupArraySorted<T, false, expr_sorted, TColumnB, is_plain_b>::AggregateFunctionGroupArraySorted;
    };

    template <typename T, bool expr_sorted, typename TColumnB, bool is_plain_b>
    class AggregateFunctionGroupArraySortedFieldType
        : public AggregateFunctionGroupArraySorted<typename T::FieldType, false, expr_sorted, TColumnB, is_plain_b>
    {
        using AggregateFunctionGroupArraySorted<typename T::FieldType, false, expr_sorted, TColumnB, is_plain_b>::
            AggregateFunctionGroupArraySorted;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<T>()); }
    };

    template <template <typename, bool, typename, bool> class AggregateFunctionTemplate, typename TColumnA, bool expr_sorted, typename TColumnB, bool is_plain_b, typename... TArgs>
    AggregateFunctionPtr
    createAggregateFunctionGroupArraySortedTypedFinal(TArgs && ... args)
    {
        return AggregateFunctionPtr(new AggregateFunctionTemplate<TColumnA, expr_sorted, TColumnB, is_plain_b>(std::forward<TArgs>(args)...));
    }

    template <bool expr_sorted = false, typename TColumnB = UInt64, bool is_plain_b = false>
    AggregateFunctionPtr
    createAggregateFunctionGroupArraySortedTyped(const DataTypes & argument_types, const Array & params, UInt64 threshold)
    {
#define DISPATCH(A, C, B) \
    if (which.idx == TypeIndex::A) \
        return createAggregateFunctionGroupArraySortedTypedFinal<C, B, expr_sorted, TColumnB, is_plain_b>(threshold, argument_types, params);
#define DISPATCH_NUMERIC(A) DISPATCH(A, AggregateFunctionGroupArraySortedNumeric, A)
        WhichDataType which(argument_types[0]);
        FOR_NUMERIC_TYPES(DISPATCH_NUMERIC)
        DISPATCH(Enum8, AggregateFunctionGroupArraySortedNumeric, Int8)
        DISPATCH(Enum16, AggregateFunctionGroupArraySortedNumeric, Int16)
        DISPATCH(Date, AggregateFunctionGroupArraySortedFieldType, DataTypeDate)
        DISPATCH(DateTime, AggregateFunctionGroupArraySortedFieldType, DataTypeDateTime)
#undef DISPATCH
#undef DISPATCH_NUMERIC

        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            return AggregateFunctionPtr(new AggregateFunctionGroupArraySorted<StringRef, true, expr_sorted, TColumnB, is_plain_b>(
                threshold, argument_types, params));
        }
        else
        {
            return AggregateFunctionPtr(new AggregateFunctionGroupArraySorted<StringRef, false, expr_sorted, TColumnB, is_plain_b>(
                threshold, argument_types, params));
        }
    }


    AggregateFunctionPtr createAggregateFunctionGroupArraySorted(
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
        else if (!params.empty())
        {
            throw Exception("Aggregate function " + name + " only supports 1 parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (argument_types.size() == 2)
        {
            if (isNumber(argument_types[1]))
            {
#define DISPATCH2(A, B) \
    if (which.idx == TypeIndex::A) \
        return createAggregateFunctionGroupArraySortedTyped<true, B>(argument_types, params, threshold);
#define DISPATCH(A) DISPATCH2(A, A)
                WhichDataType which(argument_types[1]);
                FOR_NUMERIC_TYPES(DISPATCH)
                DISPATCH2(Enum8, Int8)
                DISPATCH2(Enum16, Int16)
#undef DISPATCH
#undef DISPATCH2
                throw Exception("Invalid parameter type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            else if (argument_types[1]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            {
                return createAggregateFunctionGroupArraySortedTyped<true, StringRef, true>(argument_types, params, threshold);
            }
            else
            {
                return createAggregateFunctionGroupArraySortedTyped<true, StringRef, false>(argument_types, params, threshold);
            }
        }
        else if (argument_types.size() == 1)
        {
            return createAggregateFunctionGroupArraySortedTyped<>(argument_types, params, threshold);
        }
        else
        {
            throw Exception(
                "Aggregate function " + name + " requires one or two parameters.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }
}

void registerAggregateFunctionGroupArraySorted(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("groupArraySorted", {createAggregateFunctionGroupArraySorted, properties});
}
}
