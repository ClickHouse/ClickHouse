#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/PODArray.h>
#include <Common/BitHelpers.h>
#include <cmath>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

inline UInt8 countBits(UInt64 x)
{
#if defined(__clang__)
    return __builtin_popcountll(x);
#else
    UInt8 count = 0;
    while (x)
    {
        count += x & 1;
        x >>= 1;
    }
    return count;
#endif
}

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
    void add(UInt8 byte)
    {
        UInt32 & counter = counters[byte];
        if ((counter & generation_mask) != current_generation)
            counter = current_generation;
        counter++;
        total_count++;
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

    size_t getTotalCount() const
    {
        return total_count;
    }
};

// Implementation class for stringBytesUniq
struct StringBytesUniqImpl
{
    using ResultType = UInt8;

    static ResultType process(const char * str, size_t size)
    {
        uint64_t mask[4] = {0};
        const UInt8 * data = reinterpret_cast<const UInt8 *>(str);
        const UInt8 * end = data + size;

        for (; data < end; ++data)
        {
            UInt8 byte = *data;
            mask[byte >> 6] |= (1ULL << (byte & 0x3F));
        }

        return countBits(mask[0]) + countBits(mask[1]) + countBits(mask[2]) + countBits(mask[3]);
    }
};

// Implementation class for stringBytesEntropy
struct StringBytesEntropyImpl
{
    using ResultType = Float64;

    static ResultType process(const char * str, size_t size)
    {
        if (size == 0)
            return 0;

        ByteCounters counters;
        const UInt8 * data = reinterpret_cast<const UInt8 *>(str);
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

// Template class for string bytes functions
template <typename Impl, typename Name>
class FunctionStringBytes : public IFunction
{
public:
    static constexpr auto name = Name::name;
    using ResultType = typename Impl::ResultType;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionStringBytes>();
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}", arguments[0].type->getName(), getName());

        if constexpr (std::is_same_v<ResultType, UInt8>)
            return std::make_shared<DataTypeUInt8>();
        else if constexpr (std::is_same_v<ResultType, Float64>)
            return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnString * col_str = checkAndGetColumn<ColumnString>(column.get());

        if (!col_str)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

        auto col_res = ColumnVector<ResultType>::create();
        auto & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const ColumnString::Chars & data = col_str->getChars();
        const ColumnString::Offsets & offsets = col_str->getOffsets();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * str = reinterpret_cast<const char *>(data.data() + (i == 0 ? 0 : offsets[i - 1]));
            const size_t size = offsets[i] - (i == 0 ? 0 : offsets[i - 1]) - 1;

            vec_res[i] = Impl::process(str, size);
        }

        return col_res;
    }
};

struct NameStringBytesUniq
{
    static constexpr auto name = "stringBytesUniq";
};

struct NameStringBytesEntropy
{
    static constexpr auto name = "stringBytesEntropy";
};

using FunctionStringBytesUniq = FunctionStringBytes<StringBytesUniqImpl, NameStringBytesUniq>;
using FunctionStringBytesEntropy = FunctionStringBytes<StringBytesEntropyImpl, NameStringBytesEntropy>;

REGISTER_FUNCTION(StringBytes)
{
    factory.registerFunction<FunctionStringBytesUniq>(
        FunctionDocumentation{.description = R"(Counts the number of distinct bytes in a string.)"});

    factory.registerFunction<FunctionStringBytesEntropy>(
        FunctionDocumentation{.description = R"(Calculates Shannon's entropy of byte distribution in a string.)"});
}

}
