#pragma once

#include <array>
#include <bit>
#include <cstdint>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>
#include <Common/TargetSpecific.h>
#include "Functions/FunctionHelpers.h"
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static constexpr float Float16ToFloat32(const uint16_t & val)
{
    const uint32_t sign = static_cast<uint32_t>(val & 0x8000) << 16;
    const uint8_t exp16 = (val & 0x7c00) >> 10;
    uint16_t frac16 = val & 0x3ff;

    uint32_t exp32 = 0;
    if (__builtin_expect(exp16 == 0x1f, 0))
    {
        exp32 = 0xff;
    }
    else if (__builtin_expect(exp16 == 0, 0))
    {
        exp32 = 0;
    }
    else
    {
        exp32 = static_cast<uint32_t>(exp16) + 112;
    }

    if (__builtin_expect(exp16 == 0 && frac16 != 0, 0))
    {
        uint8_t off_set = 0;
        do
        {
            ++off_set;
            frac16 <<= 1;
        } while ((frac16 & 0x400) != 0x400);
        frac16 &= 0x3ff;
        exp32 = 113 - off_set;
    }

    uint32_t frac32 = frac16 << 13;

    uint32_t bits = 0;

    bits |= sign;
    bits |= (exp32 << 23);
    bits |= frac32;

    return std::bit_cast<float>(bits);
}

struct Lookup16Bit
{
    static inline const std::array<float, 65536> dequantize_lookup = []()
    {
        std::array<float, 65536> table{};
        for (size_t i = 0; i < 65536; ++i)
            table[i] = Float16ToFloat32(static_cast<uint16_t>(i));
        return table;
    }();
};

DECLARE_MULTITARGET_CODE(

    struct Dequantize16BitImpl { static void execute(const UInt8 * input, float * output, size_t size); };

)

template <typename Dequantize16BitImpl>
class FunctionDequantize16BitImpl : public IFunction
{
public:
    static constexpr auto name = "dequantize16Bit";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1",
                getName(),
                arguments.size());

        const DataTypeFixedString * fixed_string_type = typeid_cast<const DataTypeFixedString *>(arguments[0].get());
        if (!fixed_string_type || fixed_string_type->getN() % 2 != 0)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString with even length", getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnFixedString * col_fixed_string = nullptr;
        if (const auto * col_const = checkAndGetColumnConst<ColumnFixedString>(arguments[0].column.get()))
            col_fixed_string = checkAndGetColumn<ColumnFixedString>(col_const->getDataColumnPtr().get());
        else
            col_fixed_string = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());

        if (!col_fixed_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", getName());

        size_t fixed_string_length = col_fixed_string->getN();
        size_t array_size = fixed_string_length / 2;

        auto result_column = ColumnArray::create(ColumnFloat32::create());
        auto & result_data = typeid_cast<ColumnFloat32 &>(result_column->getData()).getData();
        auto & result_offsets = result_column->getOffsets();

        result_data.resize(input_rows_count * array_size);
        result_offsets.resize(input_rows_count);

        const auto & fixed_string_data = col_fixed_string->getChars();
        size_t offset_in_src = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Dequantize16BitImpl::execute(fixed_string_data.data() + offset_in_src, result_data.data() + i * array_size, array_size);
            offset_in_src += fixed_string_length;
            result_offsets[i] = (i + 1) * array_size;
        }

        if (isColumnConst(*arguments[0].column))
            return ColumnConst::create(std::move(result_column), input_rows_count);

        return result_column;
    }
};

class FunctionDequantize16Bit : public FunctionDequantize16BitImpl<TargetSpecific::Default::Dequantize16BitImpl>
{
public:
    explicit FunctionDequantize16Bit(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default, FunctionDequantize16BitImpl<TargetSpecific::Default::Dequantize16BitImpl>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDequantize16Bit>(context); }

private:
    ImplementationSelector<IFunction> selector;
};

} // namespace DB
