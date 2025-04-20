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

#include <bit>
#include <cmath>

namespace DB
{

struct StringBytesUniqImpl
{
    using ResultType = UInt8;

    static ResultType process(const UInt8 * data, size_t size)
    {
        UInt64 mask[4] = {0};
        const UInt8 * end = data + size;

        for (; data < end; ++data)
        {
            UInt8 byte = *data;
            mask[byte >> 6] |= (1ULL << (byte & 0x3F));
        }

        return std::popcount(mask[0]) + std::popcount(mask[1]) + std::popcount(mask[2]) + std::popcount(mask[3]);
    }
};


struct NameStringBytesUniq
{
    static constexpr auto name = "stringBytesUniq";
};


using FunctionStringBytesUniq = FunctionStringBytes<StringBytesUniqImpl, NameStringBytesUniq>;

REGISTER_FUNCTION(StringBytesUniq)
{
    factory.registerFunction<FunctionStringBytesUniq>(
        FunctionDocumentation{.description = R"(Counts the number of distinct bytes in a string.)"});
}

}
