#pragma once
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int NOT_IMPLEMENTED;
}

enum class ConvertToFixedStringExceptionMode : uint8_t
{
    Throw,
    Null
};

/** Conversion to fixed string is implemented only for strings.
  */
class FunctionToFixedString : public IFunction
{
public:
    static constexpr auto name = "toFixedString";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToFixedString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isUInt(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be unsigned integer", getName());
        if (!arguments[1].column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be constant", getName());
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is only implemented for types String and FixedString", getName());

        const size_t n = arguments[1].column->getUInt(0);
        return std::make_shared<DataTypeFixedString>(n);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto n = arguments[1].column->getUInt(0);
        return executeForN<ConvertToFixedStringExceptionMode::Throw>(arguments, n);
    }

    template<ConvertToFixedStringExceptionMode exception_mode>
    static ColumnPtr executeForN(const ColumnsWithTypeAndName & arguments, const size_t n)
    {
        const auto & column = arguments[0].column;

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (exception_mode == ConvertToFixedStringExceptionMode::Null)
        {
            col_null_map_to = ColumnUInt8::create(column->size(), false);
            vec_null_map_to = &col_null_map_to->getData();
        }

        if (const auto * column_string = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto column_fixed = ColumnFixedString::create(n);

            auto & out_chars = column_fixed->getChars();
            const auto & in_chars = column_string->getChars();
            const auto & in_offsets = column_string->getOffsets();

            out_chars.resize_fill(in_offsets.size() * n);

            for (size_t i = 0; i < in_offsets.size(); ++i)
            {
                const size_t off = i ? in_offsets[i - 1] : 0;
                const size_t len = in_offsets[i] - off - 1;
                if (len > n)
                {
                    if constexpr (exception_mode == ConvertToFixedStringExceptionMode::Throw)
                    {
                        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "String too long for type FixedString({})", toString(n));
                    }
                    else
                    {
                        (*vec_null_map_to)[i] = true;
                        continue;
                    }
                }
                memcpy(&out_chars[i * n], &in_chars[off], len);
            }

            if constexpr (exception_mode == ConvertToFixedStringExceptionMode::Null)
                return ColumnNullable::create(std::move(column_fixed), std::move(col_null_map_to));
            else
                return column_fixed;
        }
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            const auto src_n = column_fixed_string->getN();
            if (src_n > n)
            {
                if constexpr (exception_mode == ConvertToFixedStringExceptionMode::Throw)
                {
                    throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "String too long for type FixedString({})", toString(n));
                }
                else
                {
                    auto column_fixed = ColumnFixedString::create(n);
                    std::fill(vec_null_map_to->begin(), vec_null_map_to->end(), true);
                    return ColumnNullable::create(column_fixed->cloneResized(column->size()), std::move(col_null_map_to));
                }
            }

            auto column_fixed = ColumnFixedString::create(n);

            auto & out_chars = column_fixed->getChars();
            const auto & in_chars = column_fixed_string->getChars();
            const auto size = column_fixed_string->size();
            out_chars.resize_fill(size * n);

            for (size_t i = 0; i < size; ++i)
                memcpy(&out_chars[i * n], &in_chars[i * src_n], src_n);

            return column_fixed;
        }
        else
        {
            if constexpr (exception_mode == ConvertToFixedStringExceptionMode::Throw)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column: {}", column->getName());
            else
            {
                auto column_fixed = ColumnFixedString::create(n);
                std::fill(vec_null_map_to->begin(), vec_null_map_to->end(), true);
                return ColumnNullable::create(column_fixed->cloneResized(column->size()), std::move(col_null_map_to));
            }
        }
    }
};

}
