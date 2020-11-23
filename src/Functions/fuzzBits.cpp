#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <common/arithmeticOverflow.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int DECIMAL_OVERFLOW;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


namespace
{
    inline UInt8 getXorMask(UInt64 rand, double prob)
    {
        UInt8 res = 0;
        for (int i = 0; i < 8; ++i)
        {
            UInt8 rand8 = rand;
            rand >>= 8;
            res <<= 1;
            res |= (rand8 < prob * (1u << 8));
        }
        return res;
    }
    void fuzzBits(const char8_t * ptr_in, char8_t * ptr_out, size_t len, double prob)
    {
        pcg64_fast rng(randomSeed()); // TODO It is inefficient. We should use SIMD PRNG instead.

        for (size_t i = 0; i < len; ++i)
        {
            UInt64 rand = rng();
            auto mask = getXorMask(rand, prob);
            ptr_out[i] = ptr_in[i] ^ mask;
        }
    }
}


class FunctionFuzzBits : public IFunction
{
public:
    static constexpr auto name = "fuzzBits";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFuzzBits>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; } // indexing from 0

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(
                "First argument of function " + getName() + " must be String or FixedString", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1].column || !isFloat(arguments[1].type))
            throw Exception("Second argument of function " + getName() + " must be constant float", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0].type;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        auto col_in_untyped = block.getByPosition(arguments[0]).column;
        const double inverse_probability = assert_cast<const ColumnConst &>(*block.getByPosition(arguments[1]).column).getValue<double>();

        if (inverse_probability < 0.0 || 1.0 < inverse_probability)
        {
            throw Exception("Second argument of function " + getName() + " must be from `0.0` to `1.0`", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        if (const ColumnConst * col_in_untyped_const = checkAndGetColumnConstStringOrFixedString(col_in_untyped.get()))
        {
            col_in_untyped = col_in_untyped_const->getDataColumnPtr();
        }

        if (const ColumnString * col_in = checkAndGetColumn<ColumnString>(col_in_untyped.get()))
        {
            auto col_to = ColumnString::create();
            ColumnString::Chars & chars_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();

            chars_to.resize(col_in->getChars().size());
            // TODO: Maybe we can share `col_in->getOffsets()` to `offsets_to.resize` like clever pointers? They are same
            offsets_to.resize(input_rows_count);

            const auto * ptr_in = col_in->getChars().data();
            auto * ptr_to = chars_to.data();
            fuzzBits(ptr_in, ptr_to, chars_to.size(), inverse_probability);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                offsets_to[i] = col_in->getOffsets()[i];
                ptr_to[offsets_to[i] - 1] = 0;
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else if (const ColumnFixedString * col_in_fixed = checkAndGetColumn<ColumnFixedString>(col_in_untyped.get()))
        {
            const auto n = col_in_fixed->getN();
            auto col_to = ColumnFixedString::create(n);
            ColumnFixedString::Chars & chars_to = col_to->getChars();

            size_t total_size;
            if (common::mulOverflow(input_rows_count, n, total_size))
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);

            chars_to.resize(total_size);

            const auto * ptr_in = col_in_fixed->getChars().data();
            auto * ptr_to = chars_to.data();
            fuzzBits(ptr_in, ptr_to, chars_to.size(), inverse_probability);

            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

void registerFunctionFuzzBits(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFuzzBits>();
}
}
