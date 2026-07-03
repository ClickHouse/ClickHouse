#include <zlib.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>


namespace
{

template <class T>
struct CRCBase
{
    T tab[256];
    explicit CRCBase(T polynomial)
    {
        for (size_t i = 0; i < 256; ++i)
        {
            T c = static_cast<T>(i);
            for (size_t j = 0; j < 8; ++j)
                c = c & 1 ? polynomial ^ (c >> 1) : c >> 1;
            tab[i] = c;
        }
    }
};

template <class T, T polynomial>
struct CRCImpl
{
    using ReturnType = T;

    static T makeCRC(const unsigned char *buf, size_t size)
    {
        static CRCBase<ReturnType> base(polynomial);

        T crc = 0;
        for (size_t i = 0; i < size; ++i)
            crc = base.tab[(crc ^ buf[i]) & 0xff] ^ (crc >> 8);
        return crc;
    }
};

constexpr UInt64 CRC64_ECMA = 0xc96c5795d7870f42ULL;
struct CRC64ECMAImpl : public CRCImpl<UInt64, CRC64_ECMA>
{
    static constexpr auto name = "CRC64";
};

constexpr UInt32 CRC32_IEEE = 0xedb88320;
struct CRC32IEEEImpl : public CRCImpl<UInt32, CRC32_IEEE>
{
    static constexpr auto name = "CRC32IEEE";
};

struct CRC32ZLibImpl
{
    using ReturnType = UInt32;
    static constexpr auto name = "CRC32";

    static UInt32 makeCRC(const unsigned char *buf, size_t size)
    {
        return static_cast<UInt32>(crc32_z(0L, buf, size));
    }
};

}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <class Impl>
struct CRCFunctionWrapper
{
    static constexpr auto is_fixed_to_constant = true;
    using ReturnType = typename Impl::ReturnType;

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<ReturnType> & res, size_t input_rows_count)
    {
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = doCRC(data, prev_offset, offsets[i] - prev_offset);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars & data, size_t n, ReturnType & res, size_t)
    {
        res = doCRC(data, 0, n);
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<ReturnType> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            res[i] = doCRC(data, i * n, n);
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to Array argument", std::string(Impl::name));
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to UUID argument", std::string(Impl::name));
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to IPv6 argument", std::string(Impl::name));
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to IPv4 argument", std::string(Impl::name));
    }

private:
    static ReturnType doCRC(const ColumnString::Chars & buf, size_t offset, size_t size)
    {
        const unsigned char * p = reinterpret_cast<const unsigned char *>(buf.data()) + offset;
        return Impl::makeCRC(p, size);
    }
};

template <typename T>
using FunctionCRC = FunctionStringOrArrayToT<CRCFunctionWrapper<T>, T, typename T::ReturnType>;

// The same as IEEE variant, but uses 0xffffffff as initial value
// This is the default
//
// (And ZLib is used here, since it has optimized version)
using FunctionCRC32ZLib = FunctionCRC<CRC32ZLibImpl>;
// Uses CRC-32-IEEE 802.3 polynomial
using FunctionCRC32IEEE = FunctionCRC<CRC32IEEEImpl>;
// Uses CRC-64-ECMA polynomial
using FunctionCRC64ECMA = FunctionCRC<CRC64ECMAImpl>;

}

REGISTER_FUNCTION(CRC)
{
    FunctionDocumentation::Description description_crc32 = R"(
Calculates the CRC32 checksum of a string using the CRC-32-IEEE 802.3 polynomial and initial value `0xffffffff` (zlib implementation).
)";
    FunctionDocumentation::Syntax syntax_crc32 = "CRC32(s)";
    FunctionDocumentation::Arguments arguments_crc32 = {
        {"s", "String to calculate CRC32 for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_crc32 = {"Returns the CRC32 checksum of the string.", {"UInt32"}};
    FunctionDocumentation::Examples examples_crc32 = {
    {
        "Usage example",
        "SELECT CRC32('ClickHouse')",
        R"(
┌─CRC32('ClickHouse')─┐
│          1538217360 │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation_crc32 = {description_crc32, syntax_crc32, arguments_crc32, returned_value_crc32, examples_crc32, introduced_in, category};

    FunctionDocumentation::Description description_crc32ieee = R"(
Calculates the CRC32 checksum of a string using the CRC-32-IEEE 802.3 polynomial.
)";
    FunctionDocumentation::Syntax syntax_crc32ieee = "CRC32IEEE(s)";
    FunctionDocumentation::Arguments arguments_crc32ieee = {
        {"s", "String to calculate CRC32 for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_crc32ieee = {"Returns the CRC32 checksum of the string.", {"UInt32"}};
    FunctionDocumentation::Examples examples_crc32ieee = {
    {
        "Usage example",
        "SELECT CRC32IEEE('ClickHouse');",
        R"(
┌─CRC32IEEE('ClickHouse')─┐
│              3089448422 │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_crc32ieee = {description_crc32ieee, syntax_crc32ieee, arguments_crc32ieee, returned_value_crc32ieee, examples_crc32ieee, introduced_in, category};

    FunctionDocumentation::Description description_crc64 = R"(
Calculates the CRC64 checksum of a string using the CRC-64-ECMA polynomial.
)";
    FunctionDocumentation::Syntax syntax_crc64 = "CRC64(s)";
    FunctionDocumentation::Arguments arguments_crc64 = {
        {"s", "String to calculate CRC64 for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_crc64 = {"Returns the CRC64 checksum of the string.", {"UInt64"}};
    FunctionDocumentation::Examples examples_crc64 = {
    {
        "Usage example",
        "SELECT CRC64('ClickHouse');",
        R"(
┌──CRC64('ClickHouse')─┐
│ 12126588151325169346 │
└──────────────────────┘
    )"
    }
    };
    FunctionDocumentation documentation_crc64 = {description_crc64, syntax_crc64, arguments_crc64, returned_value_crc64, examples_crc64, introduced_in, category};

    factory.registerFunction<FunctionCRC32ZLib>(documentation_crc32, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionCRC32IEEE>(documentation_crc32ieee, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionCRC64ECMA>(documentation_crc64, FunctionFactory::Case::Insensitive);
}

}
