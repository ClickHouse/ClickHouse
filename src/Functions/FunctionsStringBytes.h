#pragma once

#include <Functions/IFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Common/BitHelpers.h>
#include <cmath>

namespace DB
{

// Helper class for maintaining counter array with "generation" optimization
class ByteCounters
{
private:
    static constexpr size_t COUNTERS_SIZE = 256;
    UInt32 counters[COUNTERS_SIZE] = {0};
    UInt32 current_generation = 0;
    UInt32 generation_mask = 0x80000000;
    size_t total_count = 0;

public:
    void add(UInt8 byte);
    void nextString();
    UInt32 get(UInt8 byte) const;
    size_t getTotalCount() const;
};

// Implementation class for stringBytesUniq
struct StringBytesUniqImpl
{
    using ResultType = UInt8;

    static ResultType process(const char * str, size_t size);
};

// Implementation class for stringBytesEntropy
struct StringBytesEntropyImpl
{
    using ResultType = Float64;

    static ResultType process(const char * str, size_t size);
};

struct NameStringBytesUniq
{
    static constexpr auto name = "stringBytesUniq";
};

struct NameStringBytesEntropy
{
    static constexpr auto name = "stringBytesEntropy";
};

// Template class for string bytes functions
template <typename Impl, typename Name>
class FunctionStringBytes : public IFunction
{
public:
    static constexpr auto name = Name::name;
    using ResultType = typename Impl::ResultType;

    explicit FunctionStringBytes(ContextPtr /*context*/) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionStringBytes>(context);
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;
};

using FunctionStringBytesUniq = FunctionStringBytes<StringBytesUniqImpl, NameStringBytesUniq>;
using FunctionStringBytesEntropy = FunctionStringBytes<StringBytesEntropyImpl, NameStringBytesEntropy>;

}
