#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

struct AsciiName
{
    static constexpr auto name = "ascii";
};


struct AsciiImpl
{
    static constexpr auto is_fixed_to_constant = false;
    using ReturnType = Int32;


    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<ReturnType> & res, size_t input_rows_count)
    {
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = doAscii(data, prev_offset, offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    [[noreturn]] static void vectorFixedToConstant(const ColumnString::Chars &, size_t, Int32 &, size_t)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "vectorFixedToConstant not implemented for function {}", AsciiName::name);
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<ReturnType> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            res[i] = doAscii(data, i * n, n);
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to Array argument", AsciiName::name);
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to UUID argument", AsciiName::name);
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to IPv6 argument", AsciiName::name);
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t, PaddedPODArray<ReturnType> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function {} to IPv4 argument", AsciiName::name);
    }

private:
    static Int32 doAscii(const ColumnString::Chars & buf, size_t offset, size_t size)
    {
        return size ? static_cast<ReturnType>(buf[offset]) : 0;
    }
};

using FunctionAscii = FunctionStringOrArrayToT<AsciiImpl, AsciiName, AsciiImpl::ReturnType>;

REGISTER_FUNCTION(Ascii)
{
    factory.registerFunction<FunctionAscii>(
        FunctionDocumentation{
        .description=R"(
Returns the ASCII code point of the first character of str.  The result type is Int32.

If s is empty, the result is 0. If the first character is not an ASCII character or not part of the Latin-1 Supplement range of UTF-16, the result is undefined)
        )",
        .examples{{"ascii", "SELECT ascii('234')", ""}},
        .categories{"String"}
        }, FunctionFactory::Case::Insensitive);
}

}
