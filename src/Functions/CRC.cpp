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
            T c = i;
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

struct CRC32ZLIBImpl
{
    using ReturnType = UInt32;
    static constexpr auto name = "CRC32";

    static UInt32 makeCRC(const unsigned char *buf, size_t size)
    {
        return crc32_z(0L, buf, size);
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

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<ReturnType> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = doCRC(data, prev_offset, offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars & data, size_t n, ReturnType & res) { res = doCRC(data, 0, n); }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<ReturnType> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = doCRC(data, i * n, n);
        }
    }

    [[noreturn]] static void array(const ColumnString::Offsets & /*offsets*/, PaddedPODArray<ReturnType> & /*res*/)
    {
        throw Exception("Cannot apply function " + std::string(Impl::name) + " to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container & /*offsets*/, size_t /*n*/, PaddedPODArray<ReturnType> & /*res*/)
    {
        throw Exception("Cannot apply function " + std::string(Impl::name) + " to UUID argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    static ReturnType doCRC(const ColumnString::Chars & buf, size_t offset, size_t size)
    {
        const unsigned char * p = reinterpret_cast<const unsigned char *>(buf.data()) + offset;
        return Impl::makeCRC(p, size);
    }
};

template <class T>
using FunctionCRC = FunctionStringOrArrayToT<CRCFunctionWrapper<T>, T, typename T::ReturnType>;
// The same as IEEE variant, but uses 0xffffffff as initial value
// This is the default
//
// (And zlib is used here, since it has optimized version)
using FunctionCRC32ZLIB = FunctionCRC<CRC32ZLIBImpl>;
// Uses CRC-32-IEEE 802.3 polynomial
using FunctionCRC32IEEE = FunctionCRC<CRC32IEEEImpl>;
// Uses CRC-64-ECMA polynomial
using FunctionCRC64ECMA = FunctionCRC<CRC64ECMAImpl>;

}

template <class T>
void registerFunctionCRCImpl(FunctionFactory & factory)
{
    factory.registerFunction<T>(T::name, FunctionFactory::CaseInsensitive);
}

void registerFunctionCRC(FunctionFactory & factory)
{
    registerFunctionCRCImpl<FunctionCRC32ZLIB>(factory);
    registerFunctionCRCImpl<FunctionCRC32IEEE>(factory);
    registerFunctionCRCImpl<FunctionCRC64ECMA>(factory);
}

}
