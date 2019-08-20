#include <zlib.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Calculates the CRC32 of a string
  */
struct CRC32Impl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt32> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = do_crc32(data, prev_offset, offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed_to_constant(const ColumnString::Chars & data, size_t n, UInt32 & res) { res = do_crc32(data, 0, n); }

    static void vector_fixed_to_vector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt32> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = do_crc32(data, i * n, n);
        }
    }

    [[noreturn]] static void array(const ColumnString::Offsets & /*offsets*/, PaddedPODArray<UInt32> & /*res*/)
    {
        throw Exception("Cannot apply function CRC32 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    static uint32_t do_crc32(const ColumnString::Chars & buf, size_t offset, size_t size)
    {
        const unsigned char * p = reinterpret_cast<const unsigned char *>(&buf[0]) + offset;
        return crc32(0L, p, size);
    }
};

struct NameCRC32
{
    static constexpr auto name = "CRC32";
};
using FunctionCRC32 = FunctionStringOrArrayToT<CRC32Impl, NameCRC32, UInt32>;

void registerFunctionCRC32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCRC32>(NameCRC32::name, FunctionFactory::CaseInsensitive);
}

}
