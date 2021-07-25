#include <Functions/isValidUTF8.h>

#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>

#include <cstring>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    }


    void ValidUTF8Impl::vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res)
    {
        size_t size = offsets.size();
        size_t prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = isValidUTF8(data.data() + prev_offset, offsets[i] - 1 - prev_offset);
            prev_offset = offsets[i];
        }
    }

    void ValidUTF8Impl::vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t /*n*/, UInt8 & /*res*/) {}

    void ValidUTF8Impl::vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt8> & res)
    {
        size_t size = data.size() / n;
        for (size_t i = 0; i < size; ++i)
            res[i] = isValidUTF8(data.data() + i * n, n);
    }

    [[noreturn]] void ValidUTF8Impl::array(const ColumnString::Offsets &, PaddedPODArray<UInt8> &)
    {
        throw Exception("Cannot apply function isValidUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] void ValidUTF8Impl::uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt8> &)
    {
        throw Exception("Cannot apply function isValidUTF8 to UUID argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    struct NameIsValidUTF8
    {
        static constexpr auto name = "isValidUTF8";
    };
    using FunctionValidUTF8 = FunctionStringOrArrayToT<ValidUTF8Impl, NameIsValidUTF8, UInt8>;

    void registerFunctionIsValidUTF8(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionValidUTF8>();
    }
}
