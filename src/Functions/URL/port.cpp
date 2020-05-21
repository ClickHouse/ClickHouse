#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Common/StringUtils/StringUtils.h>
#include "domain.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct ExtractPort
{
    static constexpr auto name = "port";
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt16> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = parse(data, prev_offset, offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars & data, size_t n, UInt16 & res) { res = parse(data, 0, n); }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt16> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = parse(data, i * n, n);
        }
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<UInt16> &)
    {
        throw Exception("Cannot apply function port to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    static UInt16 parse(const ColumnString::Chars & buf, size_t offset, size_t size)
    {
        const char * p = reinterpret_cast<const char *>(&buf[0]) + offset;
        const char * end = p + size;

        StringRef host = getURLHost(p, size);
        if (!host.size)
            return 0;
        if (host.size == size)
            return 0;

        p = host.data + host.size;
        if (*p++ != ':')
            return 0;

        Int64 port = 0;
        while (p < end)
        {
            if (*p == '/')
                break;
            if (!isNumericASCII(*p))
                return 0;

            port = (port * 10) + (*p - '0');
            if (port < 0 || port > UInt16(-1))
                return 0;
            ++p;
        }
        return port;
    }
};

struct NamePort
{
    static constexpr auto name = "port";
};

using FunctionPort = FunctionStringOrArrayToT<ExtractPort, NamePort, UInt16>;

void registerFunctionPort(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPort>();
}

}

