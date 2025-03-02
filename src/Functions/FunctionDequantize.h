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
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct Lookup16Bit
{
    static float float16ToFloat32(uint16_t val)
    {
        uint16_t sign = (val >> 15) & 0x1;
        uint16_t exp = (val >> 10) & 0x1F;
        uint16_t frac = val & 0x3FF;

        uint32_t fsign = static_cast<uint32_t>(sign);
        uint32_t fexp;
        uint32_t ffrac;
        if (exp == 0)
        {
            if (frac == 0)
            {
                fexp = 0;
                ffrac = 0;
                uint32_t fbits = (fsign << 31) | (fexp << 23) | ffrac;
                return std::bit_cast<float>(fbits);
            }
            else
            {
                float sgn = sign ? -1.0f : 1.0f;
                float half_denorm = 1.0f / 16384.0f;
                float mantissa = static_cast<float>(frac) / 1024.0f;
                return sgn * mantissa * half_denorm;
            }
        }
        else if (exp == 0x1F)
        {
            fexp = 0xFF;
            ffrac = (frac != 0) ? 1 : 0;
            uint32_t fbits = (fsign << 31) | (fexp << 23) | ffrac;
            return std::bit_cast<float>(fbits);
        }
        else
        {
            fexp = exp + 112;
            ffrac = static_cast<uint32_t>(frac) << 13;
            uint32_t fbits = (fsign << 31) | (fexp << 23) | ffrac;
            return std::bit_cast<float>(fbits);
        }
    }

    static inline const std::array<float, 65536> dequantize_lookup alignas(64) = []()
    {
        std::array<float, 65536> table{};
        for (size_t i = 0; i < 65536; ++i)
            table[i] = float16ToFloat32(static_cast<uint16_t>(i));
        return table;
    }();
};

static constexpr float f32FromSfP8(uint32_t sfp)
{
    const uint32_t masked_sfp = sfp & 0x7F;
    if (masked_sfp == 0)
    {
        return 0.0f;
    }
    const uint32_t sign32 = (sfp & 0x80) << 24;
    const uint32_t large_e = masked_sfp >> 6;
    const uint32_t m_bits = 2 + large_e;
    const uint32_t m = masked_sfp & ((1u << m_bits) - 1u);
    const uint32_t e = masked_sfp >> m_bits;
    const uint32_t exp32 = (104 + e + 8 * large_e) << 23;
    const uint32_t m_shift = 21 - large_e;
    const uint32_t mnt32 = m << m_shift;
    const uint32_t binary32 = sign32 | exp32 | mnt32;
    return std::bit_cast<float>(binary32);
}

struct Lookup8Bit
{
    static constexpr std::array<float, 256> dequantize_lookup alignas(64) = []() constexpr
    {
        std::array<float, 256> table{};
        for (size_t i = 0; i < 256; ++i)
            table[i] = f32FromSfP8(static_cast<uint32_t>(i));
        return table;
    }();
};

struct Lookup4Bit
{
    static constexpr float float4ToFloat32(uint8_t f4)
    {
        uint8_t sign = f4 >> 3;
        uint8_t code = f4 & 0x7;

        float value = 0.0f;
        if (code == 0)
        {
            value = 0.0f;
        }
        else
        {
            // In the FLOAT4 E2M1 format:
            // The two exponent bits are the upper two bits of the code.
            // The remaining bit is the mantissa bit.
            uint8_t exp_bits = code >> 1; // 2-bit exponent field
            uint8_t mant = code & 0x1; // 1-bit mantissa

            // When exp_bits is zero, the format is subnormal:
            //    value = mant * 2^(-1)
            // Otherwise, it is normalized:
            //    value = 2^(exp_bits - 1) * (1 + mant * 0.5)
            if (exp_bits == 0)
            {
                value = mant * 0.5f;
            }
            else
            {
                value = (1.0f + 0.5f * mant) * static_cast<float>(1 << (exp_bits - 1));
            }
        }
        return sign ? -value : value;
    }

    static inline std::array<float, 16> dequantize_lookup alignas(64) = []() constexpr
    {
        std::array<float, 16> table{};
        for (size_t i = 0; i < 16; ++i)
            table[i] = float4ToFloat32(static_cast<uint32_t>(i));
        return table;
    }();
};

template <typename Traits, typename DequantizeImpl>
class FunctionDequantizeBase : public IFunction
{
public:
    static constexpr auto name = Traits::name;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDequantizeBase>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1",
                getName(),
                arguments.size());

        const DataTypeFixedString * fixed_string_type = typeid_cast<const DataTypeFixedString *>(arguments[0].get());
        if (!fixed_string_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", getName());

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
        size_t array_size = fixed_string_length * Traits::multiplier / Traits::divisor;
        array_size = std::max(1ul, array_size);

        auto result_column = ColumnArray::create(ColumnFloat32::create());
        auto & result_data = typeid_cast<ColumnFloat32 &>(result_column->getData()).getData();
        auto & result_offsets = result_column->getOffsets();

        result_data.resize(input_rows_count * array_size);
        result_offsets.resize(input_rows_count);

        const auto & fixed_string_data = col_fixed_string->getChars();
        size_t offset_in_src = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            DequantizeImpl::execute(fixed_string_data.data() + offset_in_src, result_data.data() + i * array_size, array_size);
            offset_in_src += fixed_string_length;
            result_offsets[i] = (i + 1) * array_size;
        }

        if (isColumnConst(*arguments[0].column))
            return ColumnConst::create(std::move(result_column), input_rows_count);

        return result_column;
    }
};


}
