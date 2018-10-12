#include <Common/config.h>
#if USE_BASE64
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <IO/WriteHelpers.h>
#include <libbase64.h>


namespace DB
{
using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_DATA;
}

struct Base64Encode
{
    static constexpr auto name = "base64Encode";
    static constexpr auto buffer_size_multiplier = 5.0 / 3.0;
};

struct Base64Decode
{
    static constexpr auto name = "base64Decode";
    static constexpr auto buffer_size_multiplier = 3.0 / 4.0;
};


template <typename Func>
class FunctionBase64Conversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBase64Conversion>();
    }

    String getName() const override
    {
        return Func::name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!WhichDataType(arguments[0].type).isString())
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());

        if (!input)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        auto dst_column = ColumnString::create();
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        size_t reserve = ceil(input->getChars().size() * Func::buffer_size_multiplier + input->size());
        dst_data.resize(reserve);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = input->getOffsets();

        auto source = reinterpret_cast<const char *>(input->getChars().data());
        auto dst = reinterpret_cast<char *>(dst_data.data());
        auto dst_pos = dst;

        size_t src_offset_prev = 0;

        int codec = getCodec();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev - 1;
            size_t outlen = 0;

            if constexpr (std::is_same_v<Func, Base64Encode>)
            {
                base64_encode(source, srclen, dst_pos, &outlen, codec);
            }
            else
            {
                if (!base64_decode(source, srclen, dst_pos, &outlen, codec))
                {
                    throw Exception("Failed to " + getName() + " input '" + String(source, srclen) + "'", ErrorCodes::INCORRECT_DATA);
                }
            }

            source += srclen + 1;
            dst_pos += outlen + 1;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
        }

        dst_data.resize(dst_pos - dst);

        block.getByPosition(result).column = std::move(dst_column);
    }

private:
    static int getCodec()
    {
#if __SSE4_2__
        return BASE64_FORCE_SSE42;
#elif __SSE4_1__
        return BASE64_FORCE_SSE41;
#else
        return BASE64_FORCE_PLAIN;
#endif
    }
};
}
#endif