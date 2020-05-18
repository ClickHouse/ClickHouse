#pragma once

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
    static String getImplementationTag() { return ToString(BuildArch) + "_v2"; }
};

) // DECLARE_MULTITARGET_CODE

template <typename ToType, typename Name>
class FunctionRandomXorshift
    : public FunctionPerformanceAdaptor<FunctionRandomImpl<TargetSpecific::Default::RandXorshiftImpl, ToType, Name>>
{
public:
    FunctionRandomXorshift(const Context & context_)
        : FunctionPerformanceAdaptor<FunctionRandomImpl<TargetSpecific::Default::RandXorshiftImpl, ToType, Name>>(context_)
    {
        if constexpr (UseMultitargetCode)
        {
            registerImplementation<FunctionRandomImpl<TargetSpecific::AVX2::RandXorshiftImpl, ToType, Name>>(TargetArch::AVX2);
            registerImplementation<FunctionRandomImpl<TargetSpecific::AVX2::RandXorshiftImpl2, ToType, Name>>(TargetArch::AVX2);
        }
    }

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRandomXorshift<ToType, Name>>(context);
    }
};

}
