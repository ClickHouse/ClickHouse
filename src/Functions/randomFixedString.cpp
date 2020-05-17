#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <common/arithmeticOverflow.h>
#include <common/unaligned.h>

#include <common/defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int DECIMAL_OVERFLOW;
}


/* Generate random fixed string with fully random bytes (including zero). */
class FunctionRandomFixedString : public IFunction
{
public:
    static constexpr auto name = "randomFixedString";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRandomFixedString>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isUnsignedInteger(arguments[0].type))
            throw Exception("First argument for function " + getName() + " must be unsigned integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[0].column || !isColumnConst(*arguments[0].column))
            throw Exception("First argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);

        const size_t n = arguments[0].column->getUInt(0);
        return std::make_shared<DataTypeFixedString>(n);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto n = block.getByPosition(arguments[0]).column->getUInt(0);

        auto col_to = ColumnFixedString::create(n);
        ColumnFixedString::Chars & data_to = col_to->getChars();

        if (input_rows_count == 0)
        {
            block.getByPosition(result).column = std::move(col_to);
            return;
        }

        size_t total_size;
        if (common::mulOverflow(input_rows_count, n, total_size))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);

        /// Fill random bytes.
        data_to.resize(total_size);
        pcg64_fast rng(randomSeed()); /// TODO It is inefficient. We should use SIMD PRNG instead.

        auto * pos = data_to.data();
        auto * end = pos + data_to.size();
        while (pos < end)
        {
            unalignedStore<UInt64>(pos, rng());
            pos += sizeof(UInt64); // We have padding in column buffers that we can overwrite.
        }

        block.getByPosition(result).column = std::move(col_to);
    }
};

void registerFunctionRandomFixedString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomFixedString>();
}

}
