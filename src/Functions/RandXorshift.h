#pragma once

/// disable xorshift
#if 0

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Functions/IFunctionImpl.h>
#include <IO/WriteHelpers.h>

#include <Functions/FunctionsRandom.h>
#include <Functions/TargetSpecific.h>
#include <Functions/PerformanceAdaptors.h>

namespace DB
{

DECLARE_MULTITARGET_CODE(

struct RandXorshiftImpl
{
    static void execute(char * output, size_t size);
    static String getImplementationTag() { return ToString(BuildArch); }
};

struct RandXorshiftImpl2
{
    static void execute(char * output, size_t size);
    static String getImplementationTag() { return "v2"; }
};

) // DECLARE_MULTITARGET_CODE

template <typename ToType, typename Name>
class FunctionRandomXorshift : public FunctionRandomImpl<TargetSpecific::Default::RandXorshiftImpl, ToType, Name>
{
public:
    explicit FunctionRandomXorshift(const Context & context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            FunctionRandomImpl<TargetSpecific::Default::RandXorshiftImpl, ToType, Name>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            FunctionRandomImpl<TargetSpecific::AVX2::RandXorshiftImpl, ToType, Name>>();
        selector.registerImplementation<TargetArch::AVX2,
            FunctionRandomImpl<TargetSpecific::AVX2::RandXorshiftImpl2, ToType, Name>>();
    #endif
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        selector.selectAndExecute(block, arguments, result, input_rows_count);
    }

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRandomXorshift<ToType, Name>>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

}

#endif
