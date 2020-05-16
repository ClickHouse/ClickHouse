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

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DECLARE_MULTITARGET_CODE(

struct RandXorshiftImpl
{
    static void execute(char * output, size_t size);
    static String getImplementationTag() { return ToString(BuildArch); }
};

) // DECLARE_MULTITARGET_CODE

template <typename ToType, typename Name>
class FunctionRandomXorshift
    : public FunctionPerformanceAdaptor<FunctionRandomImpl<TargetSpecific::Default::RandXorshiftImpl, ToType, Name>>
{
public:
    FunctionRandomXorshift()
        : FunctionPerformanceAdaptor<FunctionRandomImpl<TargetSpecific::Default::RandXorshiftImpl, ToType, Name>>(
            PerformanceAdaptorOptions())
    {
        registerImplementation<FunctionRandomImpl<TargetSpecific::AVX2::RandXorshiftImpl, ToType, Name>>(TargetArch::AVX2);
    }

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRandomXorshift<ToType, Name>>();
    }
};

}
