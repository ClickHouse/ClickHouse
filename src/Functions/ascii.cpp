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


    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<ReturnType> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = doAscii(data, prev_offset, offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    [[noreturn]] static void vectorFixedToConstant(const ColumnString::Chars &  /*data*/, size_t  /*n*/, Int32 &  /*res*/)
    {
        throw Exception("vectorFixedToConstant not implemented for function " + std::string(AsciiName::name), ErrorCodes::NOT_IMPLEMENTED);
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<ReturnType> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = doAscii(data, i * n, n);
        }
    }

    [[noreturn]] static void array(const ColumnString::Offsets & /*offsets*/, PaddedPODArray<ReturnType> & /*res*/)
    {
        throw Exception("Cannot apply function " + std::string(AsciiName::name) + " to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container & /*offsets*/, size_t /*n*/, PaddedPODArray<ReturnType> & /*res*/)
    {
        throw Exception("Cannot apply function " + std::string(AsciiName::name) + " to UUID argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    static Int32 doAscii(const ColumnString::Chars & buf, size_t offset, size_t size) { return size ? static_cast<ReturnType>(buf[offset]) : 0; }
};

using FunctionAscii = FunctionStringOrArrayToT<AsciiImpl, AsciiName, AsciiImpl::ReturnType>;

REGISTER_FUNCTION(Ascii)
{
    factory.registerFunction<FunctionAscii>({}, FunctionFactory::CaseInsensitive);
}

}
