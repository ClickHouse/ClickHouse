#pragma once

#include <Common/TargetSpecific.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Functions/IFunction.h>
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

struct RandImpl
{
    /// Fill memory with random data. The memory region must be 15-bytes padded.
    static void execute(char * output, size_t size);

#if USE_MULTITARGET_CODE
    /// Assumes isArchSupported has been verified before calling
    static void executeAVX2(char * output, size_t size);
    static void executeAVX512BW(char * output, size_t size);
#endif
};

template <typename ToType, typename Name>
class FunctionRandom : public IFunction
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 0 or 1.",
                getName(), arguments.size());

        return std::make_shared<DataTypeNumber<ToType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnVector<ToType>::create();
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

        vec_to.resize(input_rows_count);
        RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(ToType));

        return col_to;
    }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRandom<ToType, Name>>(); }
};

}
