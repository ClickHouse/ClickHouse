#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Functions/IFunction.h>
#include <Functions/TargetSpecific.h>
#include <Functions/PerformanceAdaptors.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Pseudo-random number generation functions.
  * The function can be called without arguments or with one argument.
  * The argument is ignored and only serves to ensure that several calls to one function are considered different and do not stick together.
  *
  * Example:
  * SELECT rand(), rand() - will output two identical columns.
  * SELECT rand(1), rand(2) - will output two different columns.
  *
  * Non-cryptographic generators:
  *
  * rand   - linear congruential generator 0 .. 2^32 - 1.
  * rand64 - combines several rand values to get values from the range 0 .. 2^64 - 1.
  *
  * randConstant - service function, produces a constant column with a random value.
  *
  * The time is used as the seed.
  * Note: it is reinitialized for each columns.
  * This means that the timer must be of sufficient resolution to give different values to each columns.
  */

DECLARE_MULTITARGET_CODE(

struct RandImpl
{
    /// Fill memory with random data. The memory region must be 15-bytes padded.
    static void execute(char * output, size_t size);
};

) // DECLARE_MULTITARGET_CODE

template <typename RandImpl, typename ToType, typename Name>
class FunctionRandomImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 0 or 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeNumber<ToType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnVector<ToType>::create();
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);
        RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(ToType));

        return col_to;
    }
};

template <typename ToType, typename Name>
class FunctionRandom : public FunctionRandomImpl<TargetSpecific::Default::RandImpl, ToType, Name>
{
public:
    explicit FunctionRandom(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            FunctionRandomImpl<TargetSpecific::Default::RandImpl, ToType, Name>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            FunctionRandomImpl<TargetSpecific::AVX2::RandImpl, ToType, Name>>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionRandom<ToType, Name>>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

}
