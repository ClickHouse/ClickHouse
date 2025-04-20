#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/stringBytes.h>
#include <Functions/IFunction.h>
#include <Common/BitHelpers.h>
#include <Common/PODArray.h>

#include <cmath>

namespace DB
{

class ByteCounters
{
private:
    static constexpr size_t COUNTERS_SIZE = 256;
    UInt32 counters[COUNTERS_SIZE] = {0};
    UInt32 current_generation = 0;
    UInt32 generation_mask = 0x80000000;
    size_t total_count = 0;

public:
    void add(UInt8 byte)
    {
        UInt32 & counter = counters[byte];
        if ((counter & generation_mask) != current_generation)
            counter = current_generation;
        ++counter;
        ++total_count;
    }

    void nextString()
    {
        current_generation = current_generation ? 0 : generation_mask;
        total_count = 0;
    }

    UInt32 get(UInt8 byte) const
    {
        UInt32 counter = counters[byte];
        if ((counter & generation_mask) != current_generation)
            return 0;
        return counter & ~generation_mask;
    }

    size_t getTotalCount() const { return total_count; }
};


struct StringBytesEntropyImpl
{
    using ResultType = Float64;

    static ResultType process(const UInt8 * data, size_t size)
    {
        if (size == 0)
            return 0;

        ByteCounters counters;
        const UInt8 * end = data + size;

        for (; data < end; ++data)
            counters.add(*data);

        double entropy = 0.0;
        size_t total = counters.getTotalCount();

        for (size_t byte = 0; byte < 256; ++byte)
        {
            UInt32 count = counters.get(byte);
            if (count > 0)
            {
                double p = static_cast<double>(count) / total;
                entropy -= p * std::log2(p);
            }
        }

        return entropy;
    }
};

struct NameStringBytesEntropy
{
    static constexpr auto name = "stringBytesEntropy";
};

using FunctionStringBytesEntropy = FunctionStringBytes<StringBytesEntropyImpl, NameStringBytesEntropy>;

REGISTER_FUNCTION(StringBytesEntropy)
{
    factory.registerFunction<FunctionStringBytesEntropy>(
        FunctionDocumentation{.description = R"(Calculates Shannon's entropy of byte distribution in a string.)"});
}

}
