#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <fmt/format.h>
#include <Common/Base58.h>

#include <functional>


namespace DB
{

namespace Setting
{
extern const SettingsUInt64 function_base58_max_input_size;
}

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int INCORRECT_DATA;
extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Traits, typename Name>
struct BaseXXEncode
{
    static constexpr auto name = Name::name;
    static constexpr bool has_size_optimization = false;
    /// Compile-time default input-size limit (0 means "no limit"). Only base58 sets a non-zero value;
    /// the actual limit is configurable at runtime, see FunctionBaseXXConversion.
    static constexpr size_t default_max_input_size = Traits::max_input_size;

    template <bool /* with_size_optimization */>
    static void processString(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count, size_t, size_t max_input_size, const std::function<void()> & check_cancellation)
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
            if constexpr (Traits::max_input_size != 0)
                if (max_input_size != 0 && src_length > max_input_size)
                    throw Exception(
                        ErrorCodes::TOO_LARGE_STRING_SIZE,
                        "Too large input for function {}: {} bytes, maximum is {} bytes",
                        name, src_length, max_input_size);
            size_t encoded_size = Traits::perform({&src[prev_src_offset], src_length}, &dst[current_dst_offset], check_cancellation);
            prev_src_offset = current_src_offset;
            current_dst_offset += encoded_size;
            dst_offsets[row] = current_dst_offset;
        }

        dst_data.resize(current_dst_offset);
    }

    template <bool /* with_size_optimization */>
    static void processFixedString(const ColumnFixedString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count, size_t, size_t max_input_size, const std::function<void()> & check_cancellation)
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

        if constexpr (Traits::max_input_size != 0)
            if (max_input_size != 0 && N > max_input_size)
                throw Exception(
                    ErrorCodes::TOO_LARGE_STRING_SIZE,
                    "Too large input for function {}: {} bytes, maximum is {} bytes",
                    name, N, max_input_size);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t encoded_size = Traits::perform({&src[row * N], N}, &dst[current_dst_offset], check_cancellation);
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
    static constexpr bool has_size_optimization = Traits::has_size_optimization;
    /// Compile-time default input-size limit (0 means "no limit"). Only base58 sets a non-zero value;
    /// the actual limit is configurable at runtime, see FunctionBaseXXConversion.
    static constexpr size_t default_max_input_size = Traits::max_input_size;

    template <bool with_size_optimization>
    static void processString(const ColumnString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count, size_t expected_size, size_t max_input_size, const std::function<void()> & check_cancellation)
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
            if constexpr (Traits::max_input_size != 0)
            {
                if (max_input_size != 0 && src_length > max_input_size)
                {
                    if constexpr (ErrorHandling == BaseXXDecodeErrorHandling::ThrowException)
                        throw Exception(
                            ErrorCodes::TOO_LARGE_STRING_SIZE,
                            "Too large input for function {}: {} bytes, maximum is {} bytes",
                            name, src_length, max_input_size);
                    /// ReturnEmptyString: emit an empty result for this row.
                    prev_src_offset = current_src_offset;
                    dst_offsets[row] = current_dst_offset;
                    continue;
                }
            }
            std::optional<size_t> decoded_size = [&]{
                if constexpr (with_size_optimization)
                    return Traits::performWithSizeHint({&src[prev_src_offset], src_length}, &dst[current_dst_offset], expected_size, check_cancellation);
                return Traits::perform({&src[prev_src_offset], src_length}, &dst[current_dst_offset], check_cancellation);
            }();
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

    template <bool with_size_optimization>
    static void processFixedString(const ColumnFixedString & src_column, ColumnString::MutablePtr & dst_column, size_t input_rows_count, size_t expected_size, size_t max_input_size, const std::function<void()> & check_cancellation)
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

        if constexpr (Traits::max_input_size != 0)
        {
            if (max_input_size != 0 && N > max_input_size)
            {
                if constexpr (ErrorHandling == BaseXXDecodeErrorHandling::ThrowException)
                    throw Exception(
                        ErrorCodes::TOO_LARGE_STRING_SIZE,
                        "Too large input for function {}: {} bytes, maximum is {} bytes",
                        name, N, max_input_size);
                /// ReturnEmptyString: every row decodes to an empty result.
                for (size_t row = 0; row < input_rows_count; ++row)
                    dst_offsets[row] = 0;
                dst_data.resize(0);
                return;
            }
        }

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::optional<size_t> decoded_size = [&]{
                if constexpr (with_size_optimization)
                    return Traits::performWithSizeHint({&src[row * N], N}, &dst[current_dst_offset], expected_size, check_cancellation);
                return Traits::perform({&src[row * N], N}, &dst[current_dst_offset], check_cancellation);
            }();
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
class FunctionBaseXXConversion final : public IFunction
{
    static constexpr bool has_size_optimization = Func::has_size_optimization;

public:
    static constexpr auto name = Func::name;

    FunctionBaseXXConversion(QueryStatusPtr query_status_, size_t max_input_size_)
        : query_status(std::move(query_status_)), max_input_size(max_input_size_) {}

    static FunctionPtr create(ContextPtr context)
    {
        /// The input-size limit is only meaningful for the quadratic base58 conversion (the only function whose
        /// compile-time default is non-zero); for the linear base32/base64 it stays disabled regardless of the setting.
        size_t max_input_size = Func::default_max_input_size;
        if (max_input_size != 0)
            max_input_size = context->getSettingsRef()[Setting::function_base58_max_input_size];
        return std::make_shared<FunctionBaseXXConversion>(context->getProcessListElementSafe(), max_input_size);
    }
    String getName() const override { return Func::name; }
    bool isVariadic() const override { return has_size_optimization; }
    size_t getNumberOfArguments() const override { return has_size_optimization ? 0 : 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (has_size_optimization)
            return {1};
        return {};
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (has_size_optimization)
        {
            FunctionArgumentDescriptors mandatory_args{
                {"arg", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
            FunctionArgumentDescriptors optional_args{
                {"expected_size", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), nullptr, "Native unsigned integer"}};
            validateFunctionArguments(*this, arguments, mandatory_args, optional_args);
        }
        else
        {
            FunctionArgumentDescriptors args{
                {"arg", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}};
            validateFunctionArguments(*this, arguments, args);
        }

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeString>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// The generic (variable-length) Base58 conversion is quadratic in the input length and can run
        /// for a long time on a single large value, ignoring the query's time limit (which is only checked
        /// between pipeline blocks). Pass a callback so the conversion can periodically check for cancellation.
        std::function<void()> check_cancellation;
        if (query_status)
            check_cancellation = [this] { query_status->checkTimeLimit(); };

        size_t expected_size = 0;
        if constexpr (has_size_optimization)
        {
            if (arguments.size() > 1)
            {
                Field val;
                arguments[1].column->get(0, val);
                expected_size = val.safeGet<UInt64>();
            }
        }

        const ColumnPtr col = arguments[0].column;

        if (const ColumnString * col_string = checkAndGetColumn<ColumnString>(col.get()))
        {
            auto col_res = ColumnString::create();
            if constexpr (has_size_optimization)
            {
                if (expected_size > 0)
                    Func::template processString<true>(*col_string, col_res, input_rows_count, expected_size, max_input_size, check_cancellation);
                else
                    Func::template processString<false>(*col_string, col_res, input_rows_count, expected_size, max_input_size, check_cancellation);
            }
            else
            {
                Func::template processString<false>(*col_string, col_res, input_rows_count, expected_size, max_input_size, check_cancellation);
            }
            return col_res;
        }
        else if (const ColumnFixedString * col_fixed_string = checkAndGetColumn<ColumnFixedString>(col.get()))
        {
            auto col_res = ColumnString::create();
            if constexpr (has_size_optimization)
            {
                if (expected_size > 0)
                    Func::template processFixedString<true>(*col_fixed_string, col_res, input_rows_count, expected_size, max_input_size, check_cancellation);
                else
                    Func::template processFixedString<false>(*col_fixed_string, col_res, input_rows_count, expected_size, max_input_size, check_cancellation);
            }
            else
            {
                Func::template processFixedString<false>(*col_fixed_string, col_res, input_rows_count, expected_size, max_input_size, check_cancellation);
            }
            return col_res;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of first argument of function {}, must be String or FixedString",
            arguments[0].column->getName(),
            getName());
    }

private:
    QueryStatusPtr query_status;
    size_t max_input_size;
};

}
