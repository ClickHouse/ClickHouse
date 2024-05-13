#include <Columns/ColumnConst.h>
#include <Common/MemorySanitizer.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Base32.h>
#include <base/StringRef.h>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

struct Base32Encode
{
    static constexpr auto name = "base32Encode";

    static void process(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        size_t dst_column_data_size = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
            dst_column_data_size += (src_column.getDataAt(i).size + 3) / 5 * 8;

        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        dst_data.resize(dst_column_data_size + input_rows_count);
        dst_offsets.resize(input_rows_count);

        const auto * src_data = src_column.getChars().data();
        const ColumnString::Offsets & src_offsets = src_column.getOffsets();

        size_t dst_offset = 0;
        size_t src_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            encodeBase32(&src_data[src_offset], src_offsets[i] - src_offset - 1, &dst_data[dst_offset]);

            size_t cur_dst_size = (src_offsets[i] - src_offset + 3) / 5 * 8;
            src_offset = src_offsets[i];
            dst_offset += cur_dst_size;
            dst_data[dst_offset] = 0;
            dst_offset++;
            dst_offsets[i] = dst_offset;
        }
    }
};

struct Base32Decode
{
    static constexpr auto name = "base32Decode";

    static void process(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        size_t dst_column_data_size = src_column.getChars().size() + 1;

        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        dst_data.resize(dst_column_data_size);
        dst_offsets.resize(input_rows_count);

        const auto * src_data = src_column.getChars().data();
        const ColumnString::Offsets & src_offsets = src_column.getOffsets();

        size_t dst_offset = 0;
        size_t src_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::optional<size_t> decoded_size = decodeBase32(&src_data[src_offset], src_offsets[i] - src_offset - 1, &dst_data[dst_offset]);
            if (!decoded_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Base32 value, cannot be decoded");

            size_t cur_dst_size = *decoded_size;
            src_offset = src_offsets[i];
            dst_offset += cur_dst_size;
            dst_data[dst_offset] = 0;
            dst_offset++;
            dst_offsets[i] = dst_offset;
        }
        dst_data.resize(dst_offset);
    }
};

template <typename Func>
class FunctionBase32Conversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBase32Conversion>(); }
    String getName() const override { return Func::name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong number of arguments for function {}: 1 expected.", getName());

        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}. Must be String.",
                arguments[0].type->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column_string = arguments[0].column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());

        if (input == nullptr)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}, must be String",
                arguments[0].column->getName(), getName());

        auto dst_column = ColumnString::create();

        Func::process(*input, dst_column, input_rows_count);

        return dst_column;
    }
};

}

REGISTER_FUNCTION(base32Conversion)
{
    factory.registerFunction<FunctionBase32Conversion<Base32Encode>>();
    factory.registerFunction<FunctionBase32Conversion<Base32Decode>>();
}

}
