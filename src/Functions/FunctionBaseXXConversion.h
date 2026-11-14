#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <fmt/format.h>
#include <Common/Base58.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int INCORRECT_DATA;
}

template <typename Traits, typename Name>
struct BaseXXEncode
{
    static constexpr auto name = Name::name;

    static void processString(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();
        size_t const max_result_size = Traits::getBufferSize(src_column);

        dst_data.resize(max_result_size);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = src_column.getOffsets();

        const auto * src = reinterpret_cast<const char *>(src_column.getChars().data());
        auto * dst = dst_data.data();

        size_t prev_src_offset = 0;
        size_t current_dst_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t current_src_offset = src_offsets[row];
            size_t src_length = current_src_offset - prev_src_offset;
            size_t encoded_size = Traits::perform({&src[prev_src_offset], src_length}, &dst[current_dst_offset]);
            prev_src_offset = current_src_offset;
            current_dst_offset += encoded_size;
            dst_offsets[row] = current_dst_offset;
        }

        dst_data.resize(current_dst_offset);
    }

    static void processFixedString(const ColumnFixedString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();
        size_t const max_result_size = Traits::getBufferSize(src_column);

        dst_data.resize(max_result_size);
        dst_offsets.resize(input_rows_count);

        const auto * src = reinterpret_cast<const char *>(src_column.getChars().data());
        auto * dst = dst_data.data();

        size_t const N = src_column.getN();
        size_t current_dst_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t encoded_size = Traits::perform({&src[row * N], N}, &dst[current_dst_offset]);
            current_dst_offset += encoded_size;
            dst_offsets[row] = current_dst_offset;
        }

        dst_data.resize(current_dst_offset);
    }
};

enum class BaseXXDecodeErrorHandling : uint8_t
{
    ThrowException,
    ReturnEmptyString
};

template <typename Traits, typename Name, BaseXXDecodeErrorHandling ErrorHandling>
struct BaseXXDecode
{
    static constexpr auto name = Name::name;

    static void processString(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();
        size_t max_result_size = Traits::getBufferSize(src_column);

        dst_data.resize(max_result_size);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = src_column.getOffsets();

        const auto * src = reinterpret_cast<const char *>(src_column.getChars().data());
        auto * dst = dst_data.data();

        size_t prev_src_offset = 0;
        size_t current_dst_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t current_src_offset = src_offsets[row];
            size_t src_length = current_src_offset - prev_src_offset;
            std::optional<size_t> decoded_size = Traits::perform({&src[prev_src_offset], src_length}, &dst[current_dst_offset]);
            if (!decoded_size)
            {
                if constexpr (ErrorHandling == BaseXXDecodeErrorHandling::ThrowException)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Invalid {} value ({}), cannot be decoded",
                        name,
                        String(&src[prev_src_offset], src_length));
                else
                    decoded_size = 0;
            }

            prev_src_offset = current_src_offset;
            current_dst_offset += *decoded_size;
            dst_offsets[row] = current_dst_offset;
        }

        dst_data.resize(current_dst_offset);
    }

    static void processFixedString(const ColumnFixedString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();
        size_t max_result_size = Traits::getBufferSize(src_column);

        dst_data.resize(max_result_size);
        dst_offsets.resize(input_rows_count);

        const auto * src = reinterpret_cast<const char *>(src_column.getChars().data());
        auto * dst = dst_data.data();

        size_t N = src_column.getN();
        size_t current_dst_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::optional<size_t> decoded_size = Traits::perform({&src[row * N], N}, &dst[current_dst_offset]);
            if (!decoded_size)
            {
                if constexpr (ErrorHandling == BaseXXDecodeErrorHandling::ThrowException)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid {} value ({}), cannot be decoded", name, String(&src[row * N], N));
                else
                    decoded_size = 0;
            }

            current_dst_offset += *decoded_size;
            dst_offsets[row] = current_dst_offset;
        }

        dst_data.resize(current_dst_offset);
    }
};

template <typename Func>
class FunctionBaseXXConversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBaseXXConversion>(); }
    String getName() const override { return Func::name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"arg", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeString>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr col = arguments[0].column;

        if (const ColumnString * col_string = checkAndGetColumn<ColumnString>(col.get()))
        {
            auto col_res = ColumnString::create();
            Func::processString(*col_string, col_res, input_rows_count);
            return col_res;
        }
        else if (const ColumnFixedString * col_fixed_string = checkAndGetColumn<ColumnFixedString>(col.get()))
        {
            auto col_res = ColumnString::create();
            Func::processFixedString(*col_fixed_string, col_res, input_rows_count);
            return col_res;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of first argument of function {}, must be String or FixedString",
            arguments[0].column->getName(),
            getName());
    }
};

}
